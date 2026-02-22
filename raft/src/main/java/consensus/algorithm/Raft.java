package consensus.algorithm;

import consensus.membership.JointConfig;
import consensus.membership.MembershipConfig;
import consensus.membership.VoteResult;
import consensus.message.Message;
import consensus.node.NodeId;
import consensus.node.ReadState;
import consensus.storage.*;

import java.util.*;

public class Raft {
    // Identity
    private final NodeId id;

    // Persistent state
    private long term;
    private Optional<NodeId> votedFor;

    // Log
    private final RaftLog log;

    // Cluster state
    private MembershipConfig membership;

    // Timing
    private final int electionTimeout;
    private final int heartbeatTimeout;


    // Config flags
    private final ElectionProtocol protocol;
    private final boolean checkQuorum;
    private final ProposalHandleMode proposalHandleMode;

    // Limits
    private final long maxMsgSize;
    private final long maxUncommittedSize;
    private final int maxInflightMsgs;
    private final long maxInflightBytes;

    // Output buffers
    private final List<Message> messages;
    private final List<Message> messagesAfterAppend;
    private final List<Notification> notifications;
    private final List<ReadState> readStates;

    private Role role;

    public Raft(NodeId id, RaftLog log, int electionTimeout, int heartbeatTimeout, ElectionProtocol protocol, boolean checkQuorum, ProposalHandleMode proposalHandleMode, long maxMsgSize, long maxUncommittedSize, int maxInflightMsgs, long maxInflightBytes, List<Message> messages, List<Message> messagesAfterAppend, List<Notification> notifications, List<ReadState> readStates) {
        this.id = id;
        this.log = log;
        this.electionTimeout = electionTimeout;
        this.heartbeatTimeout = heartbeatTimeout;
        this.protocol = protocol;
        this.checkQuorum = checkQuorum;
        this.proposalHandleMode = proposalHandleMode;
        this.maxMsgSize = maxMsgSize;
        this.maxUncommittedSize = maxUncommittedSize;
        this.maxInflightMsgs = maxInflightMsgs;
        this.maxInflightBytes = maxInflightBytes;
        this.messages = messages;
        this.messagesAfterAppend = messagesAfterAppend;
        this.notifications = notifications;
        this.readStates = readStates;
    }

    // APIS
    public void step(Message m) throws StorageException{
        switch (role) {
            case Leader l -> handleMessage(l, m);
            case Candidate c -> handleMessage(c, m);
            case Follower f -> handleMessage(f, m);
            case Learner ln -> handleMessage(ln, m);
            case PreCandidate pc -> handleMessage(pc, m);
        }
    }

    public void tick() throws StorageException {
        switch (role) {
            case Leader l -> handleTick(l);
            case Candidate c -> handleTick(c);
            case Follower f -> handleTick(f);
            case Learner ln -> handleTick(ln);
            case PreCandidate pc -> handleTick(pc);
        }
    }

    // SENDING MESSAGES
    private void send(Message m) {
        if (
                m instanceof Message.AppendEntriesResponse ||
                m instanceof Message.RequestPreVoteResponse ||
                m instanceof Message.RequestVoteResponse
        ) {
            messagesAfterAppend.add(m);
        } else {
            messages.add(m);
        }

    }

    private void notify(Notification n) {
        notifications.add(n);
    }


    // ROLE TRANSITIONS

    /**
     * Transitions to PreCandidate — a transient role that probes whether this node
     * could win an election without actually incrementing the term.
     *
     * Unlike becomeCandidate, this does NOT change term or votedFor. That's the whole
     * point of PreVote — if the node is partitioned and can't win, it hasn't disrupted
     * the cluster with a term bump.
     *
     * Valid: Follower → PreCandidate
     * Invalid: Leader, PreCandidate → PreCandidate (throws)
     *
     * @return the newly created PreCandidate role instance
     */
    private PreCandidate becomePreCandidate() {
        if (role instanceof Leader || role instanceof PreCandidate)
            throw new IllegalStateException("Cannot transition from " +  role + " to PreCandidate role");

        if (protocol != ElectionProtocol.DUAL_ELECTION)
            throw new IllegalStateException("Cannot transition to PreCandidate role in " + protocol + " election mode");

        var preCandidate = new PreCandidate();
        role = preCandidate;
        return preCandidate;
    }

    /**
     * Transitions to Candidate — starts a real election by incrementing the term
     * and recording a self-vote. Unlike becomePreCandidate, this is a commitment:
     * even if the election fails, the term has been bumped.
     *
     * Valid: Follower, PreCandidate → Candidate
     * Invalid: Leader, Candidate → Candidate (throws)
     *
     * @return the newly created Candidate role instance
     */
    private Candidate becomeCandidate() {
        if (role instanceof Leader || role instanceof Candidate)
            throw new IllegalStateException("Cannot transition from " +  role + " to Candidate role");

        term = term + 1;
        votedFor = Optional.of(id);
        var candidate = new Candidate();
        role = candidate;
        return candidate;
    }

    /**
     * Transitions to Follower, optionally with a known leader.
     *
     * This is the most common transition — it happens when:
     *   - A higher-term message arrives (Leader/Candidate/PreCandidate steps down)
     *   - An election is lost (Candidate/PreCandidate falls back)
     *   - An AppendEntries or heartbeat from a new leader is accepted
     *
     * votedFor is only cleared when the term actually changes. If nextTerm == term,
     * the vote is preserved — clearing it would violate "vote at most once per term"
     * and could allow a node to double-vote within the same term.
     *
     * @param nextTerm  the term to adopt (may be same as current)
     * @param leader    the known leader, or null if unknown
     * @return the newly created Follower role instance
     */
    private Follower becomeFollower(long nextTerm, NodeId leader) {
        // Only clear votedFor on term change — preserves the
        // "one vote per term" invariant on same-term transitions.
        if (nextTerm != term) {
            term = nextTerm;
            votedFor = Optional.empty();
        }

        if (role instanceof Follower f) {
            f.setLeader(leader);
            f.resetElectionTimer();
            return f;
        }
        var follower = new Follower(leader);

        role = follower;
        return follower;
    }

    /**
     * Convenience overload — transitions to Follower with no known leader.
     * Used when stepping down due to a higher-term vote request or election loss,
     * where the leader identity is not yet known.
     *
     * @param nextTerm  the term to adopt
     * @return the newly created Follower role instance
     */
    private Follower becomeFollower(long nextTerm) {
        return becomeFollower(nextTerm, null);
    }

    /**
     * Transitions to Leader after winning an election.
     *
     * Initializes replication state (PeerProgress) for every peer in the cluster,
     * setting each peer's nextIndex to our last log index + 1.
     *
     * Valid: Candidate → Leader
     * Invalid: Follower → Leader (throws — must win an election first)
     *
     * Note: the caller is responsible for appending the initial placeholder entry
     * after this method returns. The placeholder is needed so the new leader can
     * commit entries from prior terms (Raft §5.4.2 — a leader can only commit
     * entries from its own term, and the no-op entry serves that purpose).
     *
     * @return the newly created Leader role instance
     */
    private Leader becomeLeader() throws StorageException {
        if (role instanceof Follower)
            throw new IllegalStateException("Cannot transition from Follower role to Leader role");

        var peers = membership.allReplicationTargets();
        var leader = new Leader(id, peers, log.lastIndex(), new Inflight.Config(maxInflightMsgs, maxInflightBytes));
        role = leader;
        return leader;
    }

    /**
     * Transitions to Learner, adopting a new term if higher.
     *
     * Mirrors becomeFollower's term/vote logic — votedFor is cleared only on
     * term change to preserve the "one vote per term" invariant.
     *
     * This is used when a Learner receives a message with a higher term. Learners
     * don't participate in elections or become candidates, but they still need to
     * track the current term and vote state correctly so they can respond to
     * RequestVote and RequestPreVote (see {@link #handleVoteReq(Learner, Message.RequestVote)}).
     *
     * @param nextTerm  the term to adopt (may be same as current)
     * @return the newly created Learner role instance
     */
    private Learner becomeLearner(long nextTerm, NodeId leader) {
        if (nextTerm != term) {
            term = nextTerm;
            votedFor = Optional.empty();
        }

        if (role instanceof Learner l) {
            l.setLeader(leader);
            return l;
        }

        var learner = new Learner(leader);

        // Same logic as becomeFollower — only clear votedFor on term change.
        role = learner;
        return learner;
    }

    private Learner becomeLearner(long nextTerm) {
        return becomeLearner(nextTerm, null);
    }

    // TICK HANDLING PER ROLE

    /**
     * Handles a tick for the Leader role. The leader has two periodic timers:
     *
     * <ol>
     *   <li><b>Quorum check (election timeout interval):</b> Verifies that a majority of
     *       peers have responded recently. If quorum is lost, the leader steps down to
     *       Follower to avoid a "zombie leader" that is partitioned from the cluster.
     *       After a quorum check, any in-progress leadership transfer is aborted — if
     *       the transfer hasn't completed within an election timeout, the transferee is
     *       likely unreachable.</li>
     *   <li><b>Heartbeat (heartbeat timeout interval):</b> Broadcasts heartbeats to all
     *       peers to maintain leader authority and prevent followers from starting
     *       elections. Only fires if the node is still a leader (the quorum check above
     *       may have caused a step-down).</li>
     * </ol>
     *
     * <p>Both timers are dispatched through {@code step()} as internal messages
     * ({@link Message.CheckQuorum}, {@link Message.TriggerHeartbeat}) to reuse the
     * standard message handling pipeline.</p>
     */
    private void handleTick(Leader l) throws StorageException {
        // Quorum check fires on election timeout interval — much longer than heartbeat.
        if (l.canCheckQuorumAfterTick()) {
            step(new Message.CheckQuorum(id));
            // If the quorum check caused a step-down, abort any in-progress transfer.
            // Otherwise abort anyway: if the transferee hasn't taken over within an
            // election timeout, it is likely unreachable.
            if (l.equals(role)) {
                l.abortLeaderTransfer();
            }
        }

        // The quorum check may have caused a step-down — bail out if no longer leader.
        if (!l.equals(role))
            return;

        // Heartbeat fires on heartbeat timeout interval — typically much shorter than
        // election timeout (e.g. 1 tick vs 10 ticks).
        if (l.canSendHeartBeatAfterTick()) {
            step(new Message.TriggerHeartbeat(id));
        }
    }

    private void handleTick(Candidate c) {

    }

    private void handleTick(Follower f) throws StorageException {
        if (f.canStartElectionAfterTick() && !log.hasUnstableSnapshot()) {
            step(new Message.TriggerElection(id));
        }
    }

    private void handleTick(Learner l) {
    }

    private void handleTick(PreCandidate pc) {

    }


    // MESSAGE HANDLING PER ROLE

    private void handleMessage(Leader l, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse _ -> {
                // Stale: this node was a PreCandidate, won the pre-election,
                // then won the actual election and became Leader. Late
                // PreVoteResponses from the pre-election phase are irrelevant.
            }
            case Message.RequestVote voteReq -> handleVoteReq(l, voteReq);
            case Message.RequestVoteResponse _ -> {
                // Stale: this node was a Candidate that already won the
                // election and became Leader. Late VoteResponses are irrelevant.
            }
            case Message.DataProposal p -> handleProposal(l, p);
            case Message.AppendEntries ae -> handleAppendEntriesForVoter(ae);
            case Message.AppendEntriesResponse aer -> handleAppendEntriesResponse(l, aer);
            case Message.InstallSnapshot is -> handleInstallSnapshotForVoter(is);
            case Message.SnapshotStatus ss ->  handleSnapshotStatus(l, ss);
            case Message.PeerUnreachable pu -> handleUnreachablePeer(l, pu);
            case Message.TriggerHeartbeat _ -> broadcastHeartbeat(l);
            case Message.CheckQuorum _ -> checkQuorum(l);
            case Message.Heartbeat hb -> handleHeartbeatForVoter(hb);
            case Message.HeartbeatResponse hbr -> handleHeartbeatResponse(l, hbr);
            case Message.TransferLeadership tl -> handleLeadershipTransfer(l, tl);
            case Message.TimeoutNow _ -> {
                // Stale: this node was a Follower that received TimeoutNow
                // as a leadership transfer target, but then won a normal
                // election (or the transfer election itself) and became Leader
                // before processing it. The transfer already completed or is
                // irrelevant now that this node is the leader.
            }
            default -> {}
        }
    }

    private void handleMessage(Candidate c, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse _ -> {
                // Stale: this node was a PreCandidate that already won the
                // pre-election and transitioned to Candidate. Late
                // PreVoteResponses from the pre-election phase are irrelevant.
            }
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.RequestVoteResponse res -> handleVoteResponse(c, res);
            case Message.DataProposal _ -> dropProposalForNoLeader();
            case Message.AppendEntries ae -> handleAppendEntriesForVoter(ae);
            case Message.InstallSnapshot is -> handleInstallSnapshotForVoter(is);
            case Message.Heartbeat hb -> handleHeartbeatForVoter(hb);
            case Message.HeartbeatResponse _ -> {
                // Stale: this node was a Leader that stepped down (higher
                // term or quorum loss) and started a new election as
                // Candidate. Late HeartbeatResponses from when it was Leader
                // are irrelevant — this node no longer tracks peer progress.
            }
            case Message.TransferLeadership _ -> {
                // Dropped: a Candidate has no leader identity and cannot
                // process or forward transfer requests. The application must
                // retry after a new leader is elected.
            }
            default -> {}
        }
    }

    private void handleMessage(PreCandidate pc, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse res -> handlePreVoteResponse(pc, res);
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.DataProposal _ -> dropProposalForNoLeader();
            case Message.AppendEntries ae -> handleAppendEntriesForVoter(ae);
            case Message.InstallSnapshot is -> handleInstallSnapshotForVoter(is);
            case Message.Heartbeat hb -> handleHeartbeatForVoter(hb);
            case Message.HeartbeatResponse _ -> {
                // Stale: this node was a Leader that stepped down (higher
                // term) and became a PreCandidate. Late HeartbeatResponses
                // from when it was Leader are irrelevant — this node no
                // longer tracks peer progress.
            }
            case Message.TransferLeadership _ -> {
                // Dropped: a PreCandidate has no leader identity and cannot
                // process or forward transfer requests. The application must
                // retry after a new leader is elected.
            }
            case Message.TimeoutNow _ -> {
                // Dropped: a PreCandidate is mid-election — it will either
                // win (become Candidate → Leader) or lose (become Follower).
                // A TimeoutNow for a leadership transfer is meaningless in
                // this transitional state.
            }
            default -> {}
        }
    }

    private void handleMessage(Follower f, Message m) throws StorageException {
        switch (m) {
            case Message.TriggerElection(_) -> startElection(f);
            case Message.RequestPreVote preVote -> handlePreVoteReq(f, preVote);
            case Message.RequestPreVoteResponse _ -> {
                // Stale: this node was a PreCandidate that discovered a
                // higher term (via AppendEntries or a vote response) and
                // stepped down to Follower. Late PreVoteResponses from the
                // abandoned pre-election are irrelevant.
            }
            case Message.RequestVote voteReq -> handleVoteReq(f, voteReq);
            case Message.RequestVoteResponse _ -> {
                // Stale: this node was a Candidate that discovered a higher
                // term and stepped down to Follower. Late VoteResponses from
                // the abandoned election are irrelevant.
            }
            case Message.DataProposal p -> handleProposal(f, p);
            case Message.AppendEntries ae -> handleAppendEntriesForVoter(ae);
            case Message.AppendEntriesResponse _ -> {
                // Stale: this node was a Leader that discovered a higher term
                // (another leader was elected) and stepped down to Follower.
                // Late AppendEntriesResponses from when it was Leader are
                // irrelevant — this node no longer tracks peer progress.
            }
            case Message.InstallSnapshot is -> handleInstallSnapshotForVoter(is);
            case Message.Heartbeat hb -> handleHeartbeatForVoter(hb);
            case Message.HeartbeatResponse _ -> {
                // Stale: this node was a Leader that discovered a higher term
                // and stepped down to Follower. Late HeartbeatResponses from
                // when it was Leader are irrelevant — this node no longer
                // tracks peer progress.
            }
            case Message.TimeoutNow _ -> transferElection(f);
            case Message.TransferLeadership tl -> handleLeadershipTransfer(f, tl);
            default -> {}
        }
    }

    private void handleMessage(Learner l, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> handlePreVoteReq(l, preVote);
            case Message.RequestVote voteReq -> handleVoteReq(l, voteReq);
            case Message.DataProposal p -> handleProposal(l, p);
            case Message.AppendEntries ae -> handleAppendEntries(l, ae);
            case Message.AppendEntriesResponse _ -> {
                // Stale: this node was a Leader that was demoted to Learner
                // by a membership change (e.g., removed from voters and added
                // as a learner). Late AppendEntriesResponses from when it was
                // Leader are irrelevant — this node no longer tracks peer
                // progress.
            }
            case Message.InstallSnapshot is -> handleInstallSnapshot(l, is);
            case Message.Heartbeat hb -> handleHeartbeat(l, hb);
            case Message.HeartbeatResponse _ -> {
                // Stale: this node was a Leader that was demoted to Learner
                // by a membership change. Late HeartbeatResponses from when
                // it was Leader are irrelevant — this node no longer tracks
                // peer progress.
            }
            case Message.TransferLeadership tl -> handleLeadershipTransfer(l, tl);
            default -> {}
        }
    }


    // ELECTION HANDLING

    /**
     * Entry point when a follower's election timeout fires.
     * Routes to pre-election or real election based on the configured protocol.
     *
     * @param f the follower whose election timeout triggered this
     */
    private void startElection(Follower f) throws StorageException {
        if (protocol == ElectionProtocol.DUAL_ELECTION) {
            preCandidateElection();
        } else {
            candidateElection(ElectionCause.TIMEOUT);
        }
    }

    /**
     * Starts a pre-election: transitions to PreCandidate and sends RequestPreVote
     * to all voters with a would-be term (current + 1) without actually incrementing it.
     *
     * The self-vote is sent as a RequestPreVoteResponse through send(), which queues
     * it in messagesAfterAppend — ensuring the role transition is persisted before
     * the vote is counted.
     */
    private void preCandidateElection() throws StorageException {
        becomePreCandidate();

        // Would-be term: what our term would be if we proceed to a real election.
        // Advertised in PreVote so voters can evaluate it, but not adopted yet.
        var newTerm = term + 1;

        for (var voter: membership.voters().allVoters()) {
            // Self-vote routed through send() so it lands in messagesAfterAppend
            // and is only processed after persistence.
            if (voter.equals(id)) {
                send(new Message.RequestPreVoteResponse(id, id, newTerm, true));
                continue;
            }

            var lastEntry = log.lastEntryId();
            send(new Message.RequestPreVote(voter, id, newTerm, lastEntry.term(), lastEntry.index()));
        }
    }

    /**
     * Starts a real election: transitions to Candidate (incrementing term, voting
     * for self) and sends {@code RequestVote} to all voters.
     *
     * <p>The {@code cause} is embedded in every {@code RequestVote} message,
     * allowing recipients to distinguish between:
     * <ul>
     *   <li>{@link ElectionCause#TIMEOUT} — normal election after timeout; voters
     *       with an active leader lease will reject</li>
     *   <li>{@link ElectionCause#LEADER_TRANSFER} — leadership transfer; voters
     *       bypass the leader lease check (the current leader authorized this)</li>
     *   <li>{@link ElectionCause#WON_PREELECTION} — won a pre-vote; proceeds
     *       to real election at the same term voters already indicated willingness
     *       to support</li>
     * </ul>
     *
     * <p>The self-vote is sent as a {@code RequestVoteResponse} through
     * {@link #send}, which queues it in {@code messagesAfterAppend} — ensuring
     * the term and vote are persisted before the vote is counted.</p>
     *
     * @param cause why this election was initiated (carried on every vote request)
     */
    private void candidateElection(ElectionCause cause) throws StorageException {
        becomeCandidate();
        for (var voter: membership.voters().allVoters()) {
            // Self-vote routed through send() so it lands in messagesAfterAppend
            // and is only processed after persistence.
            if (voter.equals(id)) {
                send(new Message.RequestVoteResponse(id, id, term, true));
                continue;
            }

            var lastEntry = log.lastEntryId();
            send(new Message.RequestVote(voter, id, term, lastEntry.term(), lastEntry.index(), cause));
        }
    }

    /**
     * Starts an election triggered by a {@code TimeoutNow} message from the
     * current leader — a graceful leadership transfer.
     *
     * <p>Always uses a real election (never pre-vote), because the leader already
     * verified the transferee is caught up. The {@link ElectionCause#LEADER_TRANSFER}
     * cause tells voters to bypass the leader lease check.</p>
     *
     * @param f the follower role that received the {@code TimeoutNow}
     */
    private void transferElection(Follower f) throws StorageException {
        candidateElection(ElectionCause.LEADER_TRANSFER);
    }

    /**
     * Handles a PreVote request received while in the Learner role.
     *
     * A learner's committed config may be stale — the cluster could have already
     * committed a config change promoting this learner to voter, but the learner
     * hasn't received it yet. If the learner refuses to vote during that window,
     * the remaining voters may not form a quorum, deadlocking the cluster.
     * So learners always participate when asked to vote.
     *
     * Example:
     *   1. Cluster has A(learner), B(voter/leader), C(voter)
     *   2. A config change promoting A to voter is committed on quorum {B, C}
     *   3. A hasn't received this config change yet — still thinks it's a learner
     *   4. B (the leader) crashes
     *   5. New voter set is {A, B, C}, quorum requires 2 votes
     *   6. C votes for itself — 1 vote. B is dead — unreachable
     *   7. C sends RequestPreVote to A
     *   8. If A refuses (thinks it's a learner) → no quorum possible → cluster deadlocks
     *   9. If A votes → C wins → becomes leader → replicates config change to A → A finally learns it's a voter
     *
     * @param l        the current learner role
     * @param preVote  the incoming PreVote request
     */
    private void handlePreVoteReq(Learner l, Message.RequestPreVote preVote) throws StorageException {
        handlePreVoteReq(preVote, l.hasLeader());
    }

    /**
     * Handles a {@code RequestPreVote} received while in the Follower role.
     *
     * <p><b>Follower lease protection (PreVote):</b> if all of the following hold,
     * the pre-vote is explicitly rejected (not silently ignored — unlike
     * {@code RequestVote}, pre-votes don't carry a term that we must adopt,
     * so a rejection with our term lets the pre-candidate learn about it):</p>
     * <ol>
     *   <li>The pre-vote is for a higher term</li>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The follower knows a leader ({@code hasLeader})</li>
     *   <li>The election timer has not reached the base election timeout</li>
     * </ol>
     *
     * <p>No {@link ElectionCause} bypass here — transfer elections always use
     * real {@code RequestVote} (never pre-vote), so a pre-vote is never a
     * transfer election.</p>
     *
     * @param f        the current follower role
     * @param preVote  the incoming PreVote request
     */
    private void handlePreVoteReq(Follower f, Message.RequestPreVote preVote) throws StorageException {
        // Lease check: recently heard from a leader → reject disruptive pre-vote.
        if (preVote.term() > term && checkQuorum && f.hasLeader() && !f.isElectionTimedOut()) {
            send(new Message.RequestPreVoteResponse(preVote.from(), id, term, false));
            return;
        }
        handlePreVoteReq(preVote, f.hasLeader());
    }

    /**
     * Core PreVote evaluation. A PreVote is granted when the node is eligible to
     * vote for the sender AND the sender's log is at least as up-to-date as ours.
     *
     * @param preVote    the incoming PreVote request
     * @param hasLeader  whether this node currently knows of a leader (lease check)
     */
    private void handlePreVoteReq(Message.RequestPreVote preVote, boolean hasLeader) throws StorageException {
        // PreVote is for a future term — since PreVote doesn't touch our term or vote,
        // we can freely indicate willingness for a hypothetical future term.
        var isUpcomingTerm = preVote.term() > term;
        var hasVoteToCast = votedFor.isEmpty();
        // We already voted for this same node — idempotent, safe to grant again.
        var alreadyVotedToSender = !hasVoteToCast && votedFor.get().equals(preVote.from());
        // Haven't voted for anyone and no known leader. If a leader exists, granting
        // would disrupt a healthy cluster (leader lease protection).
        var hasVoteAndNoLeader = hasVoteToCast && !hasLeader;
        var isSenderLogUpToDate = log.isUpToDate(preVote.lastLogTerm(), preVote.lastLogIndex());

        var voteGranted = (isUpcomingTerm || alreadyVotedToSender || hasVoteAndNoLeader) && isSenderLogUpToDate;

        // Grant: echo the message's term back. PreVote doesn't update our term, so our
        // local term may be stale. Responding with a stale term causes the campaigner
        // to discard the response as outdated.
        // Reject: send our local term so the sender learns about higher terms.
        var termToSend = voteGranted ? preVote.term() : term;
        send(new Message.RequestPreVoteResponse(preVote.from(), id, termToSend, voteGranted));
    }

    /**
     * Unconditionally rejects a PreVote. Used by Leader, Candidate, and PreCandidate
     * — roles that should not grant a PreVote because they are already leading or
     * participating in an election.
     *
     * @param preVote the incoming PreVote request to reject
     */
    private void rejectPreVote(Message.RequestPreVote preVote) {
        send(new Message.RequestPreVoteResponse(preVote.from(), id, term, false));
    }

    /**
     * Processes a PreVote response and decides the pre-election outcome.
     *
     * @param pc   the current PreCandidate role holding the vote tally
     * @param res  the incoming PreVote response
     */
    private void handlePreVoteResponse(PreCandidate pc, Message.RequestPreVoteResponse res) throws StorageException {
        pc.recordVote(res.from(), res.voteGranted());
        var result = membership.voteResult(pc.getVotes());
        switch (result) {
            // Proceed to real election — candidateElection() increments term,
            // records self-vote, and sends RequestVote to all voters.
            case VoteResult.WON -> candidateElection(ElectionCause.WON_PREELECTION);
            // Step down at our CURRENT term, not the response's term. The response
            // carries the would-be future term — using it would inflate our term,
            // defeating the purpose of PreVote.
            case VoteResult.LOST -> becomeFollower(term);
            case VoteResult.PENDING -> {}
        }
    }


    /**
     * Handles a {@code RequestVote} received by Candidate or PreCandidate.
     *
     * <p>If the vote is for a higher term, steps down to Follower first (adopting
     * the new term, clearing vote and leader state) then evaluates as a Follower
     * via {@link #handleVoteReq(Follower, Message.RequestVote)}. Same or lower
     * term votes are rejected outright — these roles have already voted for
     * themselves or are otherwise committed to their current election.</p>
     *
     * <p><b>Note:</b> The Leader role has its own dedicated handler
     * ({@link #handleVoteReq(Leader, Message.RequestVote)}) which applies
     * leader lease protection before falling through here. This method is
     * only called directly by Candidate and PreCandidate.</p>
     *
     * @param voteReq the incoming vote request
     */
    private void handleHigherTermVoteReq(Message.RequestVote voteReq) throws StorageException {
        if (voteReq.term() > term) {
            handleVoteReq(becomeFollower(voteReq.term()), voteReq);
            return;
        }
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, false));
    }

    /**
     * Handles a {@code RequestVote} received while this node is the Leader.
     *
     * <p><b>Leader lease protection:</b> if all of the following hold, the vote
     * is silently ignored — the leader does not step down, does not adopt the
     * higher term, and sends no response:</p>
     * <ol>
     *   <li>The vote is for a higher term (a same/lower-term vote is simply
     *       rejected by {@link #handleHigherTermVoteReq})</li>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The quorum check timer has not elapsed — meaning the leader recently
     *       verified it still has a responsive majority</li>
     *   <li>The election is not a {@link ElectionCause#LEADER_TRANSFER} (the
     *       leader itself authorized that election)</li>
     * </ol>
     *
     * <p>Why silence instead of a rejection: the candidate is at a higher term.
     * Any response we send carries our lower term, which the candidate would
     * drop (lower-term responses are ignored). So responding is pointless —
     * the candidate will simply time out and retry if it doesn't get enough
     * votes.</p>
     *
     * <p>If the lease check does not apply (lease expired, checkQuorum off,
     * or transfer election), this falls through to {@link #handleHigherTermVoteReq}
     * which steps down to Follower and evaluates the vote normally.</p>
     *
     * @param l       the current leader role (provides the quorum check timer)
     * @param voteReq the incoming vote request
     */
    private void handleVoteReq(Leader l, Message.RequestVote voteReq) throws StorageException {
        // Leader lease: recently verified quorum → reject disruptive votes.
        if (voteReq.term() > term && checkQuorum && !l.isQuorumCheckTimedOut() && voteReq.cause() != ElectionCause.LEADER_TRANSFER) {
            return;
        }
        handleHigherTermVoteReq(voteReq);
    }

    /**
     * Handles a RequestVote received while in the Learner role.
     * Same learner-promotion reasoning as for PreVote —
     * see {@link #handlePreVoteReq(Learner, Message.RequestPreVote)}.
     *
     * @param l        the current learner role
     * @param voteReq  the incoming vote request
     */
    private void handleVoteReq(Learner l, Message.RequestVote voteReq) throws StorageException  {
        // Higher term: adopt it first so canGrantVote runs at the correct term.
        // becomeLearner clears votedFor when term changes.
        if (voteReq.term() > term) {
            l = becomeLearner(voteReq.term());
        }
        var voteGranted = canGrantVote(voteReq.from(), voteReq.lastLogTerm(), voteReq.lastLogIndex(), l.hasLeader(), voteReq.cause());
        if (voteGranted) {
            votedFor = Optional.of(voteReq.from());
        }
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, voteGranted));
    }

    /**
     * Handles a {@code RequestVote} received while in the Follower role.
     *
     * <p><b>Follower lease protection (higher-term only):</b> before adopting a
     * higher term, the follower checks whether the leader lease is still active.
     * If all of the following hold, the vote is silently ignored — the follower
     * does not adopt the higher term:</p>
     * <ol>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The follower knows a leader ({@code hasLeader})</li>
     *   <li>The election timer has not reached the base election timeout — meaning
     *       the follower recently heard from a leader</li>
     *   <li>The election is not a {@link ElectionCause#LEADER_TRANSFER}</li>
     * </ol>
     *
     * <p><b>Why the lease check must happen before {@code becomeFollower}:</b>
     * {@code becomeFollower} clears the leader reference (the new term has no
     * confirmed leader). If we called it first, {@code hasLeader} would always
     * be false and the lease check would never trigger — completely defeating
     * the lease protection.</p>
     *
     * <p>For same-or-lower-term votes, no term change occurs so the lease
     * check is handled inside {@link #canGrantVote} via the {@code hasLeader}
     * parameter.</p>
     *
     * @param f        the current follower role
     * @param voteReq  the incoming vote request
     */
    private void handleVoteReq(Follower f, Message.RequestVote voteReq) throws StorageException {
        if (voteReq.term() > term) {
            // Lease check: recently heard from a leader → reject disruptive vote.
            if (checkQuorum && f.hasLeader() && !f.isElectionTimedOut() && voteReq.cause() != ElectionCause.LEADER_TRANSFER) {
                return;
            }
            // Adopt the higher term. becomeFollower clears votedFor and leader,
            // ensuring canGrantVote runs with a clean slate at the new term.
            f = becomeFollower(voteReq.term());
        }
        var voteGranted = canGrantVote(voteReq.from(), voteReq.lastLogTerm(), voteReq.lastLogIndex(), f.hasLeader(), voteReq.cause());

        if (voteGranted) {
            f.voteGranted();
            votedFor = Optional.of(voteReq.from());
        }
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, voteGranted));
    }

    /**
     * Core vote eligibility check for real elections. Determines whether this node
     * can grant its vote to the requesting candidate.
     *
     * <p><b>Invariant:</b> {@code term == voteReq.term()} by the time this method
     * is called. Callers must adopt any higher term (via becomeFollower/becomeLearner)
     * before invoking this, so the decision is always made at the correct term.</p>
     *
     * <p>A vote is granted when <b>both</b> conditions hold:</p>
     * <ol>
     *   <li><b>Eligible to vote for the sender:</b>
     *     <ul>
     *       <li>Haven't voted yet AND (no known leader OR this is a
     *           {@link ElectionCause#LEADER_TRANSFER} — transfer elections bypass
     *           the leader-exists check because the leader itself authorized the
     *           handoff)</li>
     *       <li>OR: already voted for this same sender (idempotent re-grant for
     *           retransmitted vote requests)</li>
     *     </ul>
     *   </li>
     *   <li><b>Sender's log is at least as up-to-date as ours</b> (Raft §5.4.1
     *       Election restriction — prevents electing a leader with a stale log)</li>
     * </ol>
     *
     * <p><b>Leader lease interaction:</b> the {@code hasLeader} parameter acts as
     * a same-term lease check. For higher-term votes, the lease check is done
     * <em>before</em> this method by the caller (see the Follower and Leader
     * vote handlers). By the time we get here after a higher-term transition,
     * {@code hasLeader} is false (becomeFollower cleared it), so the check here
     * only matters for same-term votes where no term transition occurred.</p>
     *
     * @param from          the candidate requesting the vote
     * @param lastLogTerm   the candidate's last log entry term
     * @param lastLogIndex  the candidate's last log entry index
     * @param hasLeader     whether this node currently knows of a leader
     * @param cause         why the election was initiated (controls lease bypass)
     * @return {@code true} if the vote can be granted
     */
    private boolean canGrantVote(NodeId from, long lastLogTerm, long lastLogIndex, boolean hasLeader, ElectionCause cause) throws StorageException {
        var hasVoteToCast = votedFor.isEmpty();
        // Leader lease for same-term votes: if a leader is known, only transfer
        // elections bypass this check (the leader authorized the handoff).
        var hasVoteToCastWithNoLeaderOrLeaderTransfer = hasVoteToCast && (!hasLeader || cause == ElectionCause.LEADER_TRANSFER);

        var alreadyVotedToSender = !hasVoteToCast && votedFor.get().equals(from);

        var isSenderLogUpToDate = log.isUpToDate(lastLogTerm, lastLogIndex);

        return (hasVoteToCastWithNoLeaderOrLeaderTransfer || alreadyVotedToSender) && isSenderLogUpToDate;
    }

    /**
     * Processes a RequestVoteResponse and decides the election outcome.
     *
     * If the response carries a higher term, step down immediately — someone
     * else has moved on and this election is stale.
     * Otherwise, tally the vote and act on the result:
     *   - WON: become leader and append a placeholder entry to commit entries
     *     from prior terms (Raft leader completeness requirement).
     *   - LOST: majority rejected — fall back to follower at the current term.
     *   - PENDING: still waiting for more votes, do nothing.
     *
     * @param c    the current Candidate role holding the vote tally
     * @param res  the incoming vote response
     */
    private void handleVoteResponse(Candidate c, Message.RequestVoteResponse res) throws StorageException {
        // Higher term in response: another node has advanced.
        // Step down — this election is obsolete.
        if (res.term() > term) {
            becomeFollower(res.term());
            return;
        }
        c.recordVote(res.from(), res.voteGranted());
        var result = membership.voteResult(c.getVotes());
        switch (result) {
            // Append a no-op entry so the leader can commit entries from prior terms.
            case VoteResult.WON -> appendPlaceholderEntry(becomeLeader());
            // Majority rejected — step down at current term (not response's term,
            // since we already checked higher term above).
            case VoteResult.LOST -> becomeFollower(term);
            case VoteResult.PENDING -> {}
        }
    }


    // PROPOSAL HANDLING

    /**
     * Handles a data proposal on the leader — the only role that can actually
     * append entries to the replicated log.
     *
     * Proposals are rejected (with a notification) in these cases:
     *   1. Empty data — nothing to propose
     *   2. Leader removed from cluster — a config change removed us, but we
     *      haven't stepped down yet. Accepting proposals in this window would
     *      append entries that can never reach quorum.
     *   3. Leadership transfer in progress — we're handing off to another node.
     *      Accepting proposals would extend the log and delay the transferee
     *      from catching up.
     *   4. Uncommitted size limit exceeded — back-pressure. Too many entries
     *      are in-flight (appended but not yet committed). Accepting more
     *      would risk unbounded memory growth.
     *
     * On success: entries are stamped with term/index, appended to the unstable
     * log, a self-ack is queued (via messagesAfterAppend), and AppendEntries
     * messages are broadcast to all peers.
     *
     * @param l the current leader role
     * @param p the incoming data proposal
     */
    private void handleProposal(Leader l, Message.DataProposal p) throws StorageException {
        if (p.data().isEmpty()) {
            notify(new Notification.DropProposal(ProposalDropReason.NO_DATA));
            return;
        }
        // Config change may have removed this leader — check before appending.
        if (!membership.isMember(id)) {
            notify(new Notification.DropProposal(ProposalDropReason.REMOVED_FROM_CLUSTER));
            return;
        }
        // During transfer, the leader freezes to let the transferee catch up.
        if (l.isLeaderTransferInProgress()) {
            notify(new Notification.DropProposal(ProposalDropReason.LEADER_TRANSFER_IN_PROGRESS));
            return;
        }
        if (!appendProposalEntries(l, p.data())) {
            notify(new Notification.DropProposal(ProposalDropReason.EXCEEDS_UNCOMMITTED_SIZE));
            return;
        }
        broadcastAppend(l);

    }

    /**
     * Handles a data proposal on a follower. Followers can't append to the log,
     * so they either forward the proposal to the leader or drop it.
     *
     * Forwarding puts the proposal into the messages buffer addressed to the
     * leader. The node layer is responsible for actually sending it over the
     * network. The leader will then process it as if it were a local proposal.
     *
     * Note: forwarded proposals can still be dropped by the leader (e.g., if
     * the leader is transferring, or uncommitted size is exceeded). There is no
     * acknowledgment back to the follower — the client must handle retries.
     *
     * @param f the current follower role
     * @param p the incoming data proposal
     */
    private void handleProposal(Follower f, Message.DataProposal p) {
        if (!f.hasLeader()) {
            notify(new Notification.DropProposal(ProposalDropReason.NO_LEADER));
            return;
        }

        if (proposalHandleMode == ProposalHandleMode.DROP) {
            notify(new Notification.DropProposal(ProposalDropReason.FORWARDING_DISABLED));
            return;
        }

        send(new Message.DataProposal(Optional.of(f.leaderId()), id, p.data()));
    }

    /**
     * Handles a data proposal on a learner.
     * Same forwarding logic as follower —
     * see {@link #handleProposal(Follower, Message.DataProposal)}.
     *
     * @param l the current learner role
     * @param p the incoming data proposal
     */
    private void handleProposal(Learner l, Message.DataProposal p) {
        if (!l.hasLeader()) {
            notify(new Notification.DropProposal(ProposalDropReason.NO_LEADER));
            return;
        }

        if (proposalHandleMode == ProposalHandleMode.DROP) {
            notify(new Notification.DropProposal(ProposalDropReason.FORWARDING_DISABLED));
            return;
        }

        send(new Message.DataProposal(Optional.of(l.leaderId()), id, p.data()));
    }

    /**
     * Drops a proposal because this node has no leader — used by Candidate
     * and PreCandidate, which are mid-election and can't serve proposals.
     */
    private void dropProposalForNoLeader() {
        notify(new Notification.DropProposal(ProposalDropReason.NO_LEADER));
    }

    // LOG REPLICATION

    /**
     * Core append path for the leader. Checks uncommitted size budget, appends
     * entries to the unstable log, and sends a self-ack via messagesAfterAppend.
     *
     * The self-ack is an AppendEntriesResponse addressed to ourselves. It flows
     * through messagesAfterAppend so it is only processed AFTER the entries are
     * durably persisted. When handled, it advances the leader's own match index,
     * which can then advance the commit index if quorum is reached.
     *
     * @param l       the current leader role (owns uncommitted size tracking)
     * @param entries entries to append (already stamped with term and index)
     * @return true if appended, false if uncommitted size limit exceeded
     */
    private boolean appendEntries(Leader l, List<Entry> entries) throws StorageException {
        if (!l.tryIncreaseUncommittedSize(entries)) {
            return false;
        }
        var lastIndex = log.append(entries);
        // Self-ack: the leader doesn't send AppendEntries to itself, so it
        // acknowledges its own entries via this response routed through
        // messagesAfterAppend (ensures persistence before counting).
        send(new Message.AppendEntriesResponse(id, id, term, true, lastIndex, 0, 0));
        return true;
    }

    /**
     * Sends AppendEntries to every peer (excluding self). Each peer gets a
     * tailored message based on its PeerProgress — different peers may be at
     * different log positions and receive different entries.
     *
     * @param l the current leader role
     */
    private void broadcastAppend(Leader l) throws StorageException {
        for (var target: l.peers())  {
            trySendAppend(l, target);
        }
    }

    /**
     * Convenience overload — sends AppendEntries allowing empty messages.
     * Used by broadcastAppend, heartbeat-triggered sends, and rejection retries
     * where we always want to send even if there are no new entries (to deliver
     * the commit index or probe a stalled peer).
     */
    private boolean trySendAppend(Leader l, NodeId target) throws StorageException {
        return trySendAppend(l, target, true);
    }

    /**
     * Attempts to send an AppendEntries to a single peer based on its progress.
     *
     * The message includes:
     *   - prevLogIndex / prevLogTerm: the entry just before the ones being sent,
     *     so the follower can verify log consistency
     *   - entries: the new entries to replicate (may be empty)
     *   - leaderCommit: so the follower can advance its commit index
     *
     * When the peer's inflight buffer is full (canReplicateMessages returns false),
     * an empty AppendEntries is sent instead. This acts as a probe — if all
     * inflight messages were dropped by the network, replication would stall
     * because the peer never responds and inflights never clear. The empty
     * message eventually reaches the peer, prompting a response that clears
     * the backlog.
     *
     * If the prevLogIndex has been compacted (log truncated), we can't build an
     * AppendEntries — fall back to sending a snapshot instead.
     *
     * @param l                  the current leader role
     * @param target             the peer to send to
     * @param canSendEmptyEntries if false, skip sending when there are no new entries
     *                            (used by the drain loop to avoid pointless empty messages)
     * @return true if a message was sent, false if paused or snapshot failed
     */
    private boolean trySendAppend(Leader l, NodeId target, boolean canSendEmptyEntries) throws StorageException {
        var progress = l.progress(target);
        // Flow control: don't send if this peer is paused.
        // Probe: waiting for previous response. Replicate: inflights full.
        // Snapshot: waiting for snapshot to complete.
        if (progress.isPaused())
            return false;

        try {
            // prevLogIndex and prevLogTerm: the follower verifies it has this
            // entry before accepting the new ones (log consistency check).
            var lastIndex = progress.next() - 1;
            var lastTerm = log.term(lastIndex);

            // If cannot replicate messages i.e. Inflight full — send empty AppendEntries as a heartbeat-like probe.
            // The follower will respond, clearing inflights and unblocking the pipeline.
            var nextEntries = progress.canReplicateMessages()
                    ? log.entries(progress.next(), maxMsgSize)
                    : List.<Entry>of();

            if (nextEntries.isEmpty() && !canSendEmptyEntries) {
                return false;
            }

            send(new Message.AppendEntries(target, id, term, lastTerm, lastIndex, nextEntries, log.committed()));

            // Update progress regardless of whether entries were sent.
            // In Replicate: advances next optimistically and tracks inflights.
            // In Probe: pauses after sending (waits for response before next send).
            progress.sentEntries(nextEntries.size(), Entry.calculateSize(nextEntries));
            progress.sentCommit(log.committed());
            return true;
        } catch (EntryUnavailableException | CompactedException e) {
            // prevLogIndex has been compacted — entries are no longer available.
            // The peer is too far behind for incremental replication.
            return trySendSnapshot(target, progress);
        }
    }

    /**
     * Stamps raw proposal data with the current term and sequential indexes,
     * then delegates to appendEntries for uncommitted size check, log append,
     * and self-ack.
     *
     * @param l    the current leader role
     * @param data raw byte arrays from the proposal
     * @return true if appended, false if uncommitted size limit exceeded
     */
    private boolean appendProposalEntries(Leader l, List<byte[]> data) throws StorageException {
        var nextIndex = log.lastIndex() + 1;
        var normalEntries = new ArrayList<Entry>(data.size());
        for (var entryData: data) {
            normalEntries.add(Entry.normal(term, nextIndex++, entryData));
        }
        return appendEntries(l, normalEntries);
    }

    /**
     * Appends a single empty entry when a new leader is elected.
     * This is required by Raft §5.4.2 — a leader can only commit entries from
     * its own term. The placeholder ensures there's at least one entry in the
     * current term, allowing prior-term entries to be committed indirectly.
     *
     * @param l the newly elected leader
     * @return true if appended (should always succeed for an empty entry)
     */
    private boolean appendPlaceholderEntry(Leader l) throws StorageException {
        return appendEntries(l, List.of(Entry.placeholder(term, log.lastIndex() + 1)));
    }


    // HANDLING APPEND ENTRIES RESPONSE

    /**
     * Handles an AppendEntriesResponse on the leader. This is the main
     * replication feedback loop — the leader learns what each peer has
     * accepted or rejected, and adjusts its tracking accordingly.
     *
     * Also handles the leader's own self-ack (from messagesAfterAppend),
     * which is how the leader's match index advances after its entries
     * are durably persisted.
     *
     * @param l   the current leader role
     * @param aer the incoming response
     */
    private void handleAppendEntriesResponse(Leader l, Message.AppendEntriesResponse aer) throws StorageException {
        // Stale response from a previous term — the peer has since joined
        // this term or a new one. Updating progress based on outdated
        // information could regress the match/next pointers.
        if (aer.term() < term) {
            return;
        }
        // Higher term: another leader exists. Step down immediately.
        if (aer.term() > term) {
            becomeFollower(aer.term());
            return;
        }

        var progress = l.progress(aer.from());
        
        // Unknown peer — possibly removed by a config change while the
        // response was in flight. Nothing to do.
        if (progress == null) {
            return;
        }
        // Any response proves the peer is alive — used by checkQuorum
        // to verify the leader still has a responsive majority.
        progress.setActive(true);

        if (aer.success()) {
            handleSuccessfulAppend(l, aer, progress);
        } else {
            handleFailedAppend(l, aer, progress);
        }
    }

    /**
     * Handles a successful AppendEntriesResponse — the peer accepted our entries.
     *
     * <p>This is the core of the replication feedback loop. Each successful
     * response moves the system forward in up to five ways:</p>
     *
     * <ol>
     *   <li><b>Advance the peer's match index</b> — we now know entries up to
     *       {@code aer.index()} are durably stored on that peer.</li>
     *
     *   <li><b>Promote the peer's replication state</b> — once the match point
     *       is found (Probe → Replicate), we can pipeline entries without
     *       waiting for each ack, dramatically increasing throughput.</li>
     *
     *   <li><b>Advance the global commit index</b> — if this response pushes
     *       the peer past the quorum threshold, new entries become committed.
     *       Once committed, we broadcast immediately so all nodes can apply
     *       entries to their state machines without waiting for the next
     *       heartbeat.</li>
     *
     *   <li><b>Drain queued entries</b> — after freeing inflight slots (acked
     *       entries are removed from the inflight tracker) or transitioning to
     *       Replicate (which resets inflights), there may be room to send more
     *       batches to this peer.</li>
     *
     *   <li><b>Complete leadership transfer</b> — if this peer is the transfer
     *       target and is now fully caught up, send TimeoutNow to trigger the
     *       target's immediate election.</li>
     * </ol>
     *
     * @param l        the current leader role
     * @param aer      the successful response
     * @param progress the responding peer's replication progress
     */
    private void handleSuccessfulAppend(Leader l, Message.AppendEntriesResponse aer, PeerProgress progress) throws StorageException {
        // --- Step 1: Advance match ---
        //
        // tryUpdate sets match = aer.index() if aer.index() > current match.
        // Returns false when the response is stale (aer.index() <= match) —
        // e.g., from a reordered or duplicate network message.
        //
        // Probe recovery edge case:
        // A peer in Probe state sends only one message at a time and waits
        // for the ack before sending the next. When the leader sends an empty
        // AppendEntries (no new entries to send, just confirming the current
        // match point), the peer replies with index == match. tryUpdate returns
        // false because match didn't advance. But the peer IS caught up — it
        // just has nothing new to ack.
        //
        // Without this check, the peer stays in Probe forever, throttled to
        // one message per round trip. With it, we detect "confirmed at current
        // match" and let the state promotion logic below transition it to
        // Replicate for full-speed pipelining.
        //
        // Why exclude Snapshot: if a peer is in Snapshot state (we sent it
        // a snapshot and are waiting for it to apply), a stale AppendEntries
        // response from before the snapshot could arrive with index == match.
        // Transitioning to Replicate would be premature — the peer hasn't
        // applied the snapshot yet, and the entries we'd pipeline might be
        // compacted (before the snapshot's firstIndex). So Snapshot state is
        // deliberately excluded here.
        if (!progress.tryUpdate(aer.index())) {
            boolean canRecoverFromProbe = progress.match() == aer.index() && progress.probing();
            if (!canRecoverFromProbe) {
                return;
            }
        }

        // --- Step 2: State promotion ---
        //
        // Probe/ProbePaused → Replicate:
        //   The match point is established. Switch to the fast path where we
        //   pipeline multiple batches without waiting for individual acks.
        //   Replicate uses an inflight window for backpressure instead of
        //   the one-at-a-time Probe throttle.
        //
        // Snapshot → Probe → Replicate:
        //   The peer applied the snapshot and is now responding to AppendEntries.
        //   Before switching to Replicate, we need match + 1 >= log.firstIndex():
        //   this means we can actually send the next entry after the peer's match
        //   point without hitting a compacted region. The two-step transition
        //   (Probe first, then Replicate) lets becomeProbe properly set next
        //   based on the snapshot's match, then becomeReplicate starts the
        //   inflight tracking from that point.
        //
        // Replicate/ReplicatePaused:
        //   Already in fast mode. tryUpdate freed the acked inflight entries
        //   (via removeMessagesUpTo), making room for new batches. No state
        //   change needed.
        switch (progress.state()) {
            case ReplicationState.ProbePaused _, ReplicationState.Probe _ -> progress.becomeReplicate();
            case ReplicationState.Snapshot _ when progress.match() + 1 >= log.firstIndex() -> {
                progress.becomeProbe();
                progress.becomeReplicate();
            }
            default -> {}
        }

        // --- Step 3: Advance global commit ---
        //
        // Raft's commit rule: an entry is committed when it is stored on a
        // majority of voters AND it was created in the current term. The term
        // check prevents a subtle safety issue where a leader from a previous
        // term could incorrectly commit entries from an earlier term (see
        // Raft paper §5.4.2, Figure 8).
        //
        // committedIndex returns the highest index where a majority of
        // voters have match >= that index (the median of all match values
        // in a sorted array).
        var isMessageFromAnotherNode = !aer.from().equals(id);
        var indexReplicatedAcrossMajority = membership.committedIndex(l.matchIndexer());

        if (log.tryCommit(term, indexReplicatedAcrossMajority)) {
            // Commit advanced — broadcast to all peers so they learn the new
            // commit index immediately. Without this, peers would only learn
            // about new commits via the next heartbeat, adding up to one
            // heartbeat interval of unnecessary latency.
            // TODO: also release pending ReadIndex requests here once implemented.
            broadcastAppend(l);
        } else if (isMessageFromAnotherNode && progress.canAdvanceCommit(log.committed())) {
            // Commit didn't advance globally, but this specific peer hasn't
            // been told about the current commit index yet (its committedIndex
            // in our tracking is behind our committed). Send a targeted
            // AppendEntries (possibly empty, carrying just the commit index)
            // so it can apply entries sooner.
            trySendAppend(l, aer.from());
        }

        // --- Step 4: Drain queued entries ---
        //
        // In Replicate state, tryUpdate freed inflight slots for the acked
        // entries. In the Probe → Replicate transition, inflights were reset
        // entirely. Either way, there may now be room to send more entries.
        //
        // We loop until trySendAppend returns false (no more entries to send
        // or inflight window full). canSendEmptyEntries=false avoids sending
        // no-op messages — we only want to push actual log data here.
        if (isMessageFromAnotherNode) {
            while (trySendAppend(l, aer.from(), false)) {};
        }

        // --- Step 5: Leadership transfer completion ---
        //
        // If this peer is the designated transfer target and its match index
        // now equals the leader's last log index, the target is fully caught
        // up. Send TimeoutNow to make it immediately start an election without
        // waiting for the election timeout.
        if (l.isLeaderTransferee(aer.from()) && log.lastIndex() == progress.match()) {
            send(new Message.TimeoutNow(aer.from(), id, term));
        }
    }

    /**
     * Handles a rejected AppendEntriesResponse — the peer's log didn't match
     * at prevLogIndex, so we need to backtrack and find the correct match point.
     *
     * <h3>The problem: divergent logs after partitions</h3>
     *
     * <p>Under normal conditions (no partitions), the follower's log is a
     * prefix of the leader's. The first rejection reveals where the follower's
     * log ends, and the next probe succeeds — one round trip.</p>
     *
     * <p>But after network partitions or overloaded systems, logs can diverge
     * significantly. Naively probing entry by entry in decreasing order costs:</p>
     * <pre>
     *   round trips = (length of diverging tail) × (network RTT)
     * </pre>
     * <p>which can take hours and cause outages for long divergent tails.</p>
     *
     * <p>To fix this, both the follower and the leader cooperate to skip entire
     * term ranges. The result: at most <b>O(distinct terms)</b> round trips
     * instead of O(divergent entries). The leader-side optimization is applied
     * in this method. For the follower-side counterpart, see
     * {@link #handleAppendEntriesForSameOrHigherTerm}.</p>
     *
     * <h3>Leader-side optimization (applied here)</h3>
     *
     * <p>The follower's rejection includes {@code indexHint} and
     * {@code termHint} — its best guess for where the logs might match
     * and the term at that position in its log.</p>
     *
     * <p>The leader searches its OWN log backward from {@code indexHint} for
     * the first index where the leader's term ≤ {@code termHint}.</p>
     *
     * <p><b>Why this works:</b> the follower said "my term at index N is T."
     * Log terms only increase within a log, so every index ≤ N on the follower
     * has term ≤ T. Any leader log entry with a term <i>higher</i> than T at
     * an index ≤ N can never match the follower at that position — the follower's
     * term there is at most T. Skip all of them.</p>
     *
     * <p><b>Example</b> (follower is shorter with lower terms — leader has the
     * higher divergent tail):</p>
     * <pre>
     *   idx:      1  2  3  4  5  6  7  8  9  10  11  12
     *   Leader:   1  3  3  3  5  5  5  7  7   7   7   7
     *   Follower: 1  1  1  2  2  2  2
     *   (common prefix: idx 1 only; divergence: idx 2-12)
     * </pre>
     *
     * <ol>
     *   <li>Leader probes at (idx=12, prevLogTerm=7). Follower doesn't have
     *       idx 12. Follower rejects with indexHint=7, termHint=2 (its last
     *       index and the term there).</li>
     *
     *   <li><b>Naive approach</b> — probe one by one from idx 7 downward:
     *       <pre>
     *   idx 7: leader term=5, follower term=2 → mismatch  (round trip 2)
     *   idx 6: leader term=5, follower term=2 → mismatch  (round trip 3)
     *   idx 5: leader term=5, follower term=2 → mismatch  (round trip 4)
     *   idx 4: leader term=3, follower term=2 → mismatch  (round trip 5)
     *   idx 3: leader term=3, follower term=1 → mismatch  (round trip 6)
     *   idx 2: leader term=3, follower term=1 → mismatch  (round trip 7)
     *   idx 1: leader term=1, follower term=1 → match!    (round trip 8)
     *       </pre>
     *       <b>8 round trips total.</b></li>
     *
     *   <li><b>With leader-side optimization</b> — search leader's log for
     *       term ≤ 2 starting from indexHint=7:
     *       <pre>
     *   findConflictEntryByTerm(termHint=2, indexHint=7):
     *     idx 7: leader term=5 &gt; 2, skip    (term 5 covers idx 5-7)
     *     idx 4: leader term=3 &gt; 2, skip    (term 3 covers idx 2-4)
     *     idx 1: leader term=1 ≤ 2, found!
     *       </pre>
     *       Leader jumps straight to probing idx 1 — match!
     *       <b>2 round trips total</b> (initial probe + final probe).
     *       Walked through <b>3 distinct leader terms</b> (5, 3, 1) instead
     *       of 7 individual entries.</li>
     * </ol>
     *
     * <p><b>Complexity:</b> O(distinct terms in the leader's divergent region
     * between indexHint and the match point). In this example: 3 term boundaries
     * instead of 7 entries — saving 6 round trips.</p>
     *
     * <p><b>When this optimization alone isn't enough:</b> if the follower's
     * divergent tail has <i>higher</i> terms than the leader's, the leader's
     * {@code findConflictEntryByTerm} immediately finds leader term ≤ termHint
     * at the current index (since the leader's terms are already low) — no
     * skipping occurs. It degenerates to near-linear probing. The follower-side
     * optimization in {@link #handleAppendEntriesForSameOrHigherTerm} handles
     * this case by searching the follower's own log to skip its high-term
     * divergent tail.</p>
     *
     * @param l        the current leader role
     * @param aer      the rejected response
     * @param progress the responding peer's replication progress
     */
    private void handleFailedAppend(Leader l, Message.AppendEntriesResponse aer, PeerProgress progress) throws StorageException {
        // If termHint > 0, the follower has a valid hint. Search our own log
        // for the best backtrack position (see Javadoc above for the reasoning).
        // If termHint is 0, the follower has an empty log or the index was
        // compacted — just use the raw indexHint.
        var nextProbeIndex = aer.termHint() > 0
                ? log.findConflictEntryByTerm(aer.termHint(), aer.indexHint()).index()
                : aer.indexHint();

        // tryDecrementTo adjusts the peer's next index. It guards against
        // stale rejections (e.g., from reordered messages) by verifying the
        // rejected index matches what we actually sent. If the rejection is
        // for a message we didn't send (reordering), it's ignored.
        if (progress.tryDecrementTo(aer.index(), nextProbeIndex)) {
            // If the peer was in Replicate (optimistic pipelining), the
            // rejection proves our assumption was wrong — the peer's log
            // doesn't match where we thought. Demote to Probe so we find
            // the correct match point one message at a time before resuming
            // the fast pipeline.
            if (progress.replicating()) {
                progress.becomeProbe();
            }
            // Immediately retry with the adjusted prevLogIndex — don't wait
            // for a heartbeat cycle. Fast convergence matters here.
            trySendAppend(l, aer.from());
        }
    }

    // APPEND ENTRIES — RECEIVING (Learner/Voter)

    /**
     * Handles an AppendEntries received while this node is a Learner.
     *
     * Same logic as the voter handler, except we stay as a Learner (via
     * becomeLearner) instead of transitioning to Follower.
     *
     * @param l  the current learner role
     * @param ae the incoming AppendEntries
     */
    private void handleAppendEntries(Learner l, Message.AppendEntries ae) throws StorageException {
        // Lower term: stale message from a deposed leader.
        if (ae.term() < term) {
            send(new Message.AppendEntriesResponse(ae.from(), id, term, false, ae.prevLogIndex(), 0, 0));
            return;
        }

        // Same or higher term: confirm the leader identity. becomeLearner
        // resets the leader reference (without creating a new object if
        // already a Learner) and adopts the term if higher.
        becomeLearner(ae.term(), ae.from());
        handleAppendEntriesForSameOrHigherTerm(ae);
    }

    /**
     * Handles an AppendEntries received by a Voter role (Follower, Candidate,
     * PreCandidate, or Leader).
     *
     * For Follower: confirms the leader is alive, resets election timer, and
     * processes the entries. becomeFollower reuses the existing Follower
     * instance when the term hasn't changed.
     *
     * For Candidate/PreCandidate: receiving AppendEntries at the same term
     * means another node already won the election. Step down to Follower
     * (becomeFollower handles term adoption and leader tracking).
     *
     * For Leader: receiving AppendEntries at a higher term means a new leader
     * was elected. Step down to Follower.
     *
     * @param ae the incoming AppendEntries
     */
    private void handleAppendEntriesForVoter(Message.AppendEntries ae) throws StorageException {
        // Lower term: stale message from a deposed leader.
        if (ae.term() < term) {
            send(new Message.AppendEntriesResponse(ae.from(), id, term, false, ae.prevLogIndex(), 0, 0));
            return;
        }

        // Same or higher term: transition to Follower (or reset if already one),
        // confirming ae.from() as the leader and resetting the election timer.
        becomeFollower(ae.term(), ae.from());
        handleAppendEntriesForSameOrHigherTerm(ae);
    }

    /**
     * Core AppendEntries processing after term checks and role transitions.
     * At this point, our term matches the leader's term.
     *
     * <h3>Three outcomes</h3>
     *
     * <ol>
     *   <li><b>Stale message:</b> prevLogIndex is behind our committed index.
     *       We already have those entries committed. Respond with success and
     *       our committed index so the leader can update its tracking.</li>
     *
     *   <li><b>Log consistency check passes:</b> we have an entry at
     *       prevLogIndex with the matching prevLogTerm. Append entries, advance
     *       commit index, and respond with success.</li>
     *
     *   <li><b>Log consistency check fails:</b> our log diverges at
     *       prevLogIndex. Respond with a rejection and provide optimized hints
     *       so the leader can backtrack efficiently. See the follower-side
     *       optimization below.</li>
     * </ol>
     *
     * <h3>Follower-side optimization (applied in outcome 3)</h3>
     *
     * <h4>The problem: large divergent follower logs</h4>
     *
     * <p>When logs diverge after partitions, the leader-side optimization
     * (see {@link #handleFailedAppend(Leader, Message.AppendEntriesResponse, PeerProgress)})
     * works well when the <i>leader's</i> divergent tail has higher terms
     * than the follower's — it skips entire term ranges in the leader's log.
     * But it breaks down when the situation is reversed: the <i>follower's</i>
     * divergent tail has higher terms than the leader's.</p>
     *
     * <p>This can happen after crash/recovery sequences: a follower was
     * briefly elected leader at a higher term, appended entries, then crashed
     * before committing — those uncommitted entries remain as a high-term
     * divergent tail. In this scenario, the leader-side optimization provides
     * no benefit because the leader's terms in the divergent region are
     * already low (≤ every follower termHint), so its backward search
     * immediately lands at the current index every time — zero skipping,
     * one index per round trip, O(divergent entries).</p>
     *
     * <p><b>Example</b> (follower has a high-term divergent tail):</p>
     * <pre>
     *   idx:      1  2  3  4  5  6  7  8  9  10  11  12
     *   Leader:   1  2  2  3  3  3  3  3  3   3   3   8
     *   Follower: 1  2  2  4  4  5  5  6  6   7   7   7
     *   (common prefix: idx 1-3; divergence: idx 4-12)
     * </pre>
     *
     * <p><b>With leader-side optimization only</b> — showing the
     * degeneration to linear probing:</p>
     *
     * <ol>
     *   <li><b>Round 1:</b> Leader probes at (idx=12, prevLogTerm=8).
     *       Follower: term at 12 is 7 ≠ 8. Rejects with indexHint=12, termHint=7.
     *       <br/>Leader-side: findConflictEntryByTerm(termHint=7, indexHint=12):
     *       <pre>
     *     idx 12: leader term=8 &gt; 7, skip
     *     idx 11: leader term=3 ≤ 7, found!
     *       </pre>
     *       Leader skipped 1 entry. Probes at idx 11 next.</li>
     *
     *   <li><b>Round 2:</b> Leader probes at (idx=11, prevLogTerm=3).
     *       Follower: term at 11 is 7 ≠ 3. Rejects with indexHint=11, termHint=7.
     *       <br/>Leader-side: findConflictEntryByTerm(termHint=7, indexHint=11):
     *       <pre>
     *     idx 11: leader term=3 ≤ 7, found immediately!
     *       </pre>
     *       <b>No skipping</b> — leader's term (3) is already ≤ termHint (7).
     *       Leader moves to idx 10.</li>
     *
     *   <li><b>Round 3:</b> Leader probes at (idx=10, prevLogTerm=3).
     *       Follower: term at 10 is 7 ≠ 3. Rejects with indexHint=10, termHint=7.
     *       <br/>Leader-side: findConflictEntryByTerm(termHint=7, indexHint=10):
     *       <pre>
     *     idx 10: leader term=3 ≤ 7, found immediately!
     *       </pre>
     *       <b>No skipping again.</b> Leader moves to idx 9.</li>
     *
     *   <li><b>Rounds 4-9:</b> Same pattern for idx 9, 8, 7, 6, 5, 4.
     *       At each index, leader term is 3, follower's termHint is always
     *       ≥ 4 (follower terms are 4, 5, 6, 7). The leader-side search
     *       returns the current index immediately every time — zero skipping.
     *       <pre>
     *   Round 4: idx=9,  leader term=3 ≤ 6 → found immediately
     *   Round 5: idx=8,  leader term=3 ≤ 6 → found immediately
     *   Round 6: idx=7,  leader term=3 ≤ 5 → found immediately
     *   Round 7: idx=6,  leader term=3 ≤ 5 → found immediately
     *   Round 8: idx=5,  leader term=3 ≤ 4 → found immediately
     *   Round 9: idx=4,  leader term=3 ≤ 4 → found immediately
     *       </pre></li>
     *
     *   <li><b>Round 10:</b> Leader probes at idx 3: leader term=2, follower
     *       term=2 → match!</li>
     * </ol>
     *
     * <p><b>~10 round trips</b> — the leader-side optimization provided
     * zero benefit from Round 2 onward because the leader's terms (all 3)
     * are consistently ≤ the follower's termHints (4, 5, 6, 7). The search
     * always lands at the current index. This is still O(divergent entries)
     * — 9 divergent entries, ~10 round trips.</p>
     *
     * <h4>The solution: follower-side term-range skipping</h4>
     *
     * <p>The follower can do better than naively returning its last index
     * and term. The key insight: the leader's {@code prevLogTerm} reveals
     * information about the leader's entire log prefix — since terms only
     * increase within a log, every entry before prevLogIndex in the leader's
     * log has term ≤ prevLogTerm.</p>
     *
     * <p>So the follower searches backward through its OWN log for the
     * largest index where its term ≤ prevLogTerm. Any follower entry with
     * a term <i>higher</i> than prevLogTerm can never match the leader at
     * that position (the leader's term there is at most prevLogTerm). Skip
     * them all.</p>
     *
     * <p><b>Same example, now with follower-side optimization:</b></p>
     * <pre>
     *   idx:      1  2  3  4  5  6  7  8  9  10  11  12
     *   Leader:   1  2  2  3  3  3  3  3  3   3   3   8
     *   Follower: 1  2  2  4  4  5  5  6  6   7   7   7
     * </pre>
     *
     * <ol>
     *   <li><b>Round 1:</b> Leader probes at (idx=12, prevLogTerm=8).
     *       Follower: term at 12 is 7 ≠ 8. Follower-side:
     *       <pre>
     *   findConflictEntryByTerm(prevLogTerm=8, idx=12):
     *     idx 12: our term=7 ≤ 8, found immediately!
     *       </pre>
     *       No skipping (prevLogTerm too high). Return indexHint=12, termHint=7.
     *       <br/>Leader-side: skips idx 12 (term 8 &gt; 7), lands on idx 11
     *       (term 3 ≤ 7). Probes at idx 11 next.</li>
     *
     *   <li><b>Round 2:</b> Leader probes at (idx=11, prevLogTerm=3).
     *       Follower: term at 11 is 7 ≠ 3. Follower-side:
     *       <pre>
     *   findConflictEntryByTerm(prevLogTerm=3, idx=11):
     *     idx 11: our term=7 &gt; 3, skip   (term 7 covers idx 10-11)
     *     idx  9: our term=6 &gt; 3, skip   (term 6 covers idx 8-9)
     *     idx  7: our term=5 &gt; 3, skip   (term 5 covers idx 6-7)
     *     idx  5: our term=4 &gt; 3, skip   (term 4 covers idx 4-5)
     *     idx  3: our term=2 ≤ 3, found!
     *       </pre>
     *       Return indexHint=3, termHint=2.
     *       Jumped from idx 11 to idx 3 — skipped 8 entries in one round trip.</li>
     *
     *   <li><b>Round 3:</b> Leader probes at idx 3: leader term=2, our
     *       term=2 → <b>match!</b></li>
     * </ol>
     *
     * <p><b>3 round trips total</b> vs ~10 with leader-side only.</p>
     *
     * <p><b>Complexity:</b> O(distinct terms in the follower's divergent
     * region). In this example: walked through 4 distinct follower terms
     * (7, 6, 5, 4) to skip from idx 11 to idx 3 in one round trip.
     * Together with the leader-side optimization (which skips ranges where
     * the <i>leader's</i> terms are too high), both sides cooperate to
     * converge in at most O(distinct terms) round trips across the divergent
     * region of either log.</p>
     *
     * @param ae the incoming AppendEntries (term already verified)
     */
    private void handleAppendEntriesForSameOrHigherTerm(Message.AppendEntries ae) throws StorageException {
        // prevLogIndex is already committed — this is a stale or duplicate
        // message. The follower already has those entries (and possibly more).
        // Respond with success so the leader can advance its tracking.
        // We send our committed index as the last matched index because
        // everything up to committed is guaranteed to match the leader.
        if (ae.prevLogIndex() < log.committed()) {
            send(new Message.AppendEntriesResponse(ae.from(), id, term, true, log.committed(), 0, 0));
            return;
        }

        // tryAppend performs the Raft log consistency check:
        //   "Does this follower have an entry at prevLogIndex with prevLogTerm?"
        // If yes: find where new entries diverge from existing ones (if at all),
        // truncate any conflicting suffix, append the new entries, and advance
        // commit to min(leaderCommit, lastNewEntryIndex).
        var lastIndex = log.tryAppend(ae.prevLogTerm(), ae.prevLogIndex(), ae.entries(), ae.leaderCommit());

        if (lastIndex.isPresent()) {
            send(new Message.AppendEntriesResponse(ae.from(), id, term, true, lastIndex.getAsLong(), 0, 0));
            return;
        }

        // Log consistency check failed — our log diverges from the leader's.
        // Provide optimized hints via findConflictEntryByTerm so the leader
        // can find the match point in O(distinct terms) instead of O(entries).
        // See this method's Javadoc for the full follower-side optimization
        // explanation with a worked example.
        var indexHint = Math.min(ae.prevLogIndex(), log.lastIndex());
        var conflictingEntry = log.findConflictEntryByTerm(ae.prevLogTerm(), indexHint);
        send(new Message.AppendEntriesResponse(ae.from(), id, term, false, ae.prevLogIndex(), conflictingEntry.term(), conflictingEntry.index()));
    }

    // QUORUM CHECK

    /**
     * Verifies that the leader still has the support of a majority of the cluster.
     *
     * <p>This is invoked on every election timeout interval (not heartbeat interval) to
     * detect network partitions where the leader can no longer reach a quorum. Without
     * this check, a partitioned leader would continue to accept proposals indefinitely
     * even though those entries can never be committed.</p>
     *
     * <p><b>How it works:</b></p>
     * <ol>
     *   <li>Collects the {@code active} flag from each peer's {@link PeerProgress}.
     *       A peer is marked active whenever the leader receives any message from it
     *       (heartbeat response, append response, etc.). The leader itself is always
     *       counted as active.</li>
     *   <li>After collecting, <em>all peer flags are reset to inactive</em> (the leader's
     *       own flag stays active). This means each peer must respond again before the next
     *       quorum check or it will be counted as inactive.</li>
     *   <li>The collected votes are passed to
     *       {@link consensus.membership.MembershipConfig#voteResult(Map) voteResult()} which
     *       determines if a majority of voters are active.</li>
     *   <li>If the result is anything other than {@code WON} (including {@code PENDING},
     *       which means not enough active peers to form a definitive majority), the leader
     *       steps down to Follower.</li>
     * </ol>
     *
     * <p><b>Example — 5-node cluster {A, B, C, D, E}, A is leader:</b></p>
     * <pre>
     *   Before check: A=active(self), B=active, C=active, D=inactive, E=inactive
     *   Votes: {A=true, B=true, C=true, D=false, E=false}
     *   Result: 3/5 active → WON → leader stays
     *   After check: all peer flags reset → B=inactive, C=inactive, D=inactive, E=inactive
     *
     *   If B and C also become unresponsive before next check:
     *   Votes: {A=true, B=false, C=false, D=false, E=false}
     *   Result: 1/5 active → LOST → leader steps down
     * </pre>
     */
    private void checkQuorum(Leader l) {
        var votes = l.getQuorumVotesAndDeactivate();
        if (membership.voteResult(votes) != VoteResult.WON) {
            becomeFollower(term);
        }
    }

    // HANDLING HEARTBEAT

    /**
     * Broadcasts a heartbeat to every peer in the cluster (excluding the leader itself).
     *
     * <p>Heartbeats serve three purposes:</p>
     * <ol>
     *   <li><b>Leader authority:</b> Resets each follower's election timer, preventing
     *       unnecessary elections while the leader is alive.</li>
     *   <li><b>Commit advancement:</b> Carries the leader's commit index (capped to
     *       each follower's match) so followers can apply committed entries even when
     *       there are no new log entries to replicate.</li>
     *   <li><b>Liveness signal:</b> Heartbeat responses mark peers as active for
     *       quorum checks (see {@link #checkQuorum(Leader)}).</li>
     * </ol>
     *
     * @see #sendHeartbeat(Leader, NodeId) for the per-peer logic
     */
    private void broadcastHeartbeat(Leader l) {
        for (var peer: l.peers()) {
            sendHeartbeat(l, peer);
        }
    }

    /**
     * Sends a heartbeat to a single peer.
     *
     * <p>The commit index carried in the heartbeat is
     * {@code min(progress.match(), log.committed())}. This cap is critical: the leader
     * must not tell a follower to commit beyond what it has actually replicated. If the
     * follower's log is behind, advancing its commit index past its log would cause it
     * to apply entries it does not have.</p>
     *
     * <p>After sending, we record the committed index we communicated via
     * {@link PeerProgress#sentCommit(long)} so that subsequent AppendEntries can avoid
     * redundantly re-sending the same commit index.</p>
     *
     * @param l      the current leader role state
     * @param target the peer to send the heartbeat to
     */
    private void sendHeartbeat(Leader l, NodeId target) {
        var progress = l.progress(target);
        // Peer may have been removed by a membership change.
        if (progress == null) {
            return;
        }

        var commitIndex = Math.min(progress.match(), log.committed());
        send(new Message.Heartbeat(target, id, term, commitIndex));
        progress.sentCommit(commitIndex);
    }

    /**
     * Handles a heartbeat received by a <b>voter</b> (Follower, Candidate, or PreCandidate).
     *
     * <p>Follows the same term-handling pattern as
     * {@link #handleAppendEntriesForVoter(Message.AppendEntries)}:</p>
     * <ul>
     *   <li><b>Lower term:</b> Stale heartbeat from a deposed leader. Respond so the
     *       sender discovers the higher term and steps down. No state change.</li>
     *   <li><b>Same or higher term:</b> Legitimate heartbeat from the current (or new)
     *       leader. Transition to Follower (resets election timer, confirms leader),
     *       advance local commit index, and respond.</li>
     * </ul>
     *
     * <p>For Candidates and PreCandidates receiving a same-or-higher term heartbeat,
     * {@code becomeFollower} causes them to abandon their election — a leader already
     * exists for this term.</p>
     */
    private void handleHeartbeatForVoter(Message.Heartbeat hb) throws StorageException {
        // Lower term: stale heartbeat from a deposed leader.
        // Respond so the sender discovers the higher term and steps down.
        if (hb.term() < term) {
            send(new Message.HeartbeatResponse(hb.from(), id, term));
            return;
        }

        becomeFollower(hb.term(), hb.from());
        log.commitTo(hb.leaderCommit());
        send(new Message.HeartbeatResponse(hb.from(), id, term));
    }

    /**
     * Handles a heartbeat received by a <b>Learner</b>.
     *
     * <p>Mirrors {@link #handleHeartbeatForVoter(Message.Heartbeat)} but calls
     * {@link #becomeLearner(long, NodeId)} instead of {@code becomeFollower} to
     * preserve the node's learner (non-voting) status.</p>
     */
    private void handleHeartbeat(Learner l, Message.Heartbeat hb) throws StorageException {
        // Lower term: stale heartbeat from a deposed leader.
        // Respond so the sender discovers the higher term and steps down.
        if (hb.term() < term) {
            send(new Message.HeartbeatResponse(hb.from(), id, term));
            return;
        }

        becomeLearner(hb.term(), hb.from());
        log.commitTo(hb.leaderCommit());
        send(new Message.HeartbeatResponse(hb.from(), id, term));
    }

    /**
     * Handles a heartbeat response received by the Leader.
     *
     * <p><b>Term handling:</b></p>
     * <ul>
     *   <li><b>Lower term:</b> Stale response from a previous term. Silently dropped —
     *       no useful information can be extracted from it.</li>
     *   <li><b>Higher term:</b> Another node has been elected leader. Step down to
     *       Follower.</li>
     *   <li><b>Same term:</b> Process the response (below).</li>
     * </ul>
     *
     * <p><b>Same-term processing:</b></p>
     * <ol>
     *   <li><b>Mark active:</b> The peer is alive and reachable — mark it active for
     *       quorum checks (see {@link #checkQuorum(Leader)}).</li>
     *   <li><b>Resume flow:</b> If the peer was paused (e.g., Probe waiting for a
     *       response, or Replicate with full in-flights), resume it. A heartbeat
     *       response is proof that the network path is working, so it's safe to
     *       resume sending.</li>
     *   <li><b>Trigger replication:</b> If the peer is in Probe state (may be
     *       recovering from an unreachable report) or is behind the leader's log, send
     *       an AppendEntries. This allows the peer to:
     *       <ul>
     *         <li>Transition from Probe back to Replicate on a successful response.</li>
     *         <li>Recover from situations where all in-flight messages were dropped
     *             (the heartbeat response proves connectivity is restored).</li>
     *         <li>Catch up on entries it hasn't received yet.</li>
     *       </ul>
     *       Note: if the peer is in Snapshot state, {@code trySendAppend} is a no-op
     *       since snapshot-state peers are always paused.</li>
     * </ol>
     */
    private void handleHeartbeatResponse(Leader l, Message.HeartbeatResponse hbr) throws StorageException {
        // Lower term: stale response from a previous term. Drop silently.
        if (hbr.term() < term) {
            return;
        }

        // Higher term: another leader exists. Step down.
        if (hbr.term() > term) {
            becomeFollower(hbr.term());
            return;
        }

        var progress = l.progress(hbr.from());

        // Peer may have been removed by a membership change.
        if (progress == null) {
            return;
        }

        progress.setActive(true);
        progress.resumeStateIfPaused();

        if (progress.probing() || progress.match() < log.lastIndex()) {
            trySendAppend(l, hbr.from());
        }

        // TODO: handle read-only requests
    }

    // SNAPSHOT HANDLING

    /**
     * Sends a snapshot to a peer that is too far behind for log-based replication.
     *
     * Only attempted if the peer is recently active — there's no point sending
     * a potentially large snapshot to a peer that isn't responding.
     *
     * On success, the peer transitions to Snapshot state, pausing all
     * AppendEntries until the snapshot is applied and the peer responds.
     *
     * @param target   the peer to send to
     * @param progress the peer's replication progress
     * @return true if snapshot was sent, false if peer inactive or snapshot unavailable
     */
    private boolean trySendSnapshot(NodeId target, PeerProgress progress) throws StorageException {
        if (!progress.isActive()) {
            return false;
        }
        try {
            var snapshot = log.snapshot();
            if (snapshot.isEmpty()) {
                throw new IllegalStateException("Cannot send snapshot in empty state");
            }
            progress.becomeSnapshot(snapshot.index());
            send(new Message.InstallSnapshot(target, id, term, snapshot));
            return true;
        } catch (SnapshotUnavailableException e) {
            // Snapshot not ready yet — the storage may be preparing it asynchronously.
            // The peer will remain in its current state and we'll retry later
            // (typically on the next heartbeat cycle).
            return false;
        }
    }

    /**
     * Handles an InstallSnapshot received while this node is a Learner.
     *
     * <p>Same logic as the voter handler
     * ({@link #handleInstallSnapshotForVoter(Message.InstallSnapshot)}),
     * except we stay as a Learner (via {@code becomeLearner}) instead of
     * transitioning to Follower. Learners are the most common snapshot
     * recipients — they're typically new nodes catching up on the log.</p>
     *
     * @param l  the current learner role
     * @param is the incoming InstallSnapshot
     */
    private void handleInstallSnapshot(Learner l, Message.InstallSnapshot is) throws StorageException {
        // Lower term: stale message from a deposed leader. Respond so
        // the sender discovers the higher term and steps down.
        if (is.term() < term) {
            send(new Message.AppendEntriesResponse(is.from(), id, term, false, is.snapshot().index(), 0, 0));
            return;
        }

        // Same or higher term: confirm the leader identity. becomeLearner
        // resets the leader reference and adopts the term if higher.
        becomeLearner(is.term(), is.from());
        restore(is);
    }

    /**
     * Handles an InstallSnapshot received by a voter role (Follower, Candidate,
     * PreCandidate, or Leader).
     *
     * <p>Follows the same pattern as
     * {@link #handleAppendEntriesForVoter(Message.AppendEntries)}:
     * reject lower-term messages, then call {@code becomeFollower} for both
     * same-term and higher-term messages before processing. This ensures the
     * election timer is reset (the snapshot proves the leader is alive) and
     * the leader identity is confirmed.</p>
     *
     * <p>For Candidate/PreCandidate: receiving a snapshot proves a leader
     * exists in this term — the node steps down to Follower and applies the
     * snapshot, just like it would for an AppendEntries.</p>
     *
     * @param is the incoming InstallSnapshot
     */
    private void handleInstallSnapshotForVoter(Message.InstallSnapshot is) throws StorageException {
        // Lower term: stale message from a deposed leader. Respond so
        // the sender discovers the higher term and steps down.
        if (is.term() < term) {
            send(new Message.AppendEntriesResponse(is.from(), id, term, false, is.snapshot().index(), 0, 0));
            return;
        }

        // Same or higher term: transition to Follower (or reset if already one),
        // confirming is.from() as the leader and resetting the election timer.
        becomeFollower(is.term(), is.from());
        restore(is);
    }

    /**
     * Shared response logic after term checks and role transitions.
     *
     * <p>Delegates to {@link #tryRestore(Snapshot)} and sends an
     * AppendEntriesResponse back to the leader in both cases:</p>
     *
     * <ul>
     *   <li><b>Restored</b> (tryRestore returned true): the log was replaced
     *       by the snapshot. Respond with {@code lastIndex()} — the leader
     *       uses this to advance the peer's match index to the snapshot point.</li>
     *   <li><b>Not restored</b> (tryRestore returned false): the snapshot was
     *       stale, redundant, or rejected. Respond with {@code committed} —
     *       tells the leader where this node currently stands so it can resume
     *       normal replication from that point.</li>
     * </ul>
     *
     * <p>Both responses are marked as success ({@code true}). Even when the
     * snapshot isn't applied, this node is operational and the leader should
     * update its progress tracking accordingly. This is different from
     * AppendEntries rejection (log divergence) where the response carries
     * {@code false} and divergence hints.</p>
     *
     * @param is the InstallSnapshot message (used for leader address and snapshot data)
     */
    private void restore(Message.InstallSnapshot is) throws StorageException {
        if (tryRestore(is.snapshot())) {
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.lastIndex(), 0, 0));
        } else {
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.committed(), 0, 0));
        }
    }

    /**
     * Attempts to restore this node's state from a snapshot.
     *
     * <p>This is the core snapshot-restore logic with three rejection cases
     * and one success case:</p>
     *
     * <ol>
     *   <li><b>Already committed:</b> the snapshot's index is at or behind our
     *       committed index. We already have all those entries committed and
     *       possibly applied — the snapshot is stale. Return false.</li>
     *
     *   <li><b>Not a member:</b> this node is not in the snapshot's membership
     *       (not a voter, not a learner). This is a defense-in-depth check —
     *       it should never happen in normal operation, but if it did (e.g.,
     *       the node was removed from the cluster), applying the snapshot would
     *       leave the node in an inconsistent state. Return false.</li>
     *
     *   <li><b>Log already matches:</b> we have an entry at the snapshot's
     *       index with the same term. Our log is consistent with the snapshot
     *       up to that point — no need to replace it. Just fast-forward the
     *       commit index to the snapshot's index. Return false.</li>
     *
     *   <li><b>Full restore:</b> our log diverges from the snapshot (or we
     *       don't have entries at that index). Replace the entire log with
     *       the snapshot and adopt its membership configuration. Return true.</li>
     * </ol>
     *
     * <p>Note: when membership configuration changes are implemented, the full
     * restore path must also rebuild the progress tracker from the snapshot's
     * membership (switchToConfig). Without this, a node that later becomes
     * leader would have a stale progress tracker.</p>
     *
     * @param snapshot the snapshot to restore from
     * @return true if the log was replaced (full restore), false if the
     *         snapshot was stale, rejected, or the commit was fast-forwarded
     */
    private boolean tryRestore(Snapshot snapshot) throws StorageException {
        if (snapshot.index() <= log.committed()) {
            return false;
        }

        if (!snapshot.membership().isMember(id)) {
            return false;
        }

        // Our log already has this entry with the correct term — we're
        // consistent up to the snapshot point. Just advance the commit
        // index without replacing the log (avoids discarding entries
        // beyond the snapshot that are still valid).
        if (log.matchTerm(snapshot.term(), snapshot.index())) {
            log.commitTo(snapshot.index());
            return false;
        }

        log.restore(snapshot);
        membership = snapshot.membership();
        // TODO: rebuild progress tracker from membership (switchToConfig)
        return true;
    }

    /**
     * Handles local feedback about the outcome of a snapshot delivery.
     *
     * <p>This is NOT a network message — it's local feedback from the
     * application/transport layer on the same node that sent the snapshot.
     * After the leader sends an {@link Message.InstallSnapshot}, the peer
     * transitions to Snapshot state and all AppendEntries are paused. This
     * method unblocks the peer when the transport reports back.</p>
     *
     * <p>Two outcomes:</p>
     * <ul>
     *   <li><b>Success:</b> the snapshot was delivered. Transition to Probe
     *       starting from {@code snapshotIndex + 1}. The peer will send an
     *       AppendEntriesResponse after it applies the snapshot — that
     *       response will confirm the match point and trigger a transition
     *       to Replicate for fast pipelining.</li>
     *   <li><b>Failure:</b> the delivery failed (network error, peer rejected
     *       it, etc.). Reset to Probe starting from {@code match + 1} — we
     *       go back to the last confirmed match point since the snapshot
     *       was never applied.</li>
     * </ul>
     *
     * <p>In both cases, the peer is paused after transitioning to Probe.
     * On success, we wait for the peer's AppendEntriesResponse before sending
     * new entries. On failure, we wait for the next heartbeat cycle before
     * retrying.</p>
     *
     * @param l  the current leader role
     * @param ss the snapshot delivery status
     */
    private void handleSnapshotStatus(Leader l, Message.SnapshotStatus ss) {
        var progress = l.progress(ss.peer());

        // Peer removed from config while snapshot was in flight, or
        // duplicate/stale status for a peer that already moved out of
        // Snapshot state (e.g., received another message that transitioned it).
        if (progress == null || !(progress.state() instanceof ReplicationState.Snapshot)) {
            return;
        }

        if (ss.success()) {
            progress.becomeProbe();
        } else {
            progress.snapshotFailed();
        }

        // Pause after both success and failure — don't send AppendEntries
        // until we have a reason to resume (peer's response or next heartbeat).
        progress.pauseStateIfResumed();
    }

    /**
     * Handles local feedback that a peer is unreachable.
     *
     * <p>This is NOT a network message — it's local feedback from the
     * transport layer when it fails to deliver a message to a peer
     * (connection refused, timeout, etc.).</p>
     *
     * <p>Only affects peers in Replicate state. During optimistic pipelining,
     * the leader sends multiple AppendEntries without waiting for responses.
     * If the peer becomes unreachable, those pipelined messages were likely
     * lost. Demoting to Probe switches to conservative one-at-a-time sending,
     * which is more appropriate for an unreliable connection.</p>
     *
     * <p>If the peer is already in Probe (already conservative) or Snapshot
     * (waiting for snapshot to complete), this is a no-op — those states
     * are already prepared for communication failures.</p>
     *
     * @param l  the current leader role
     * @param pu the unreachable peer notification
     */
    private void handleUnreachablePeer(Leader l, Message.PeerUnreachable pu) {
        var progress = l.progress(pu.peer());
        // Peer removed from config while message was in flight.
        if (progress == null) {
            return;
        }
        if (progress.replicating()) {
            progress.becomeProbe();
        }
    }

    // ─── LEADERSHIP TRANSFER ──────────────────────────────────────────────

    /**
     * Handles a leadership transfer request at the Leader.
     *
     * <p>The application sends {@code TransferLeadership} to gracefully move
     * leadership to a specific voter. The leader does not respond to the
     * application — the transfer outcome is observable by watching who becomes
     * the next leader (via a new leader's {@code AppendEntries}).</p>
     *
     * <p><b>Validation sequence:</b></p>
     * <ol>
     *   <li>Self-transfer — transferring to yourself is a no-op</li>
     *   <li>Unknown/learner — only voters in the current config can become leader</li>
     *   <li>Duplicate — if a transfer to the same target is already in progress,
     *       don't restart the process</li>
     * </ol>
     *
     * <p>Once validated, {@link Leader#transferLeadership(NodeId)} sets the target
     * and resets the quorum check timer, giving the transferee a full election-timeout
     * window to catch up. If a transfer was already in progress to a <em>different</em>
     * target, it is implicitly aborted — the new target overwrites the old one.</p>
     *
     * <p><b>Catch-up or immediate handoff:</b></p>
     * <ul>
     *   <li><b>Caught up</b> ({@code progress.match() == lastIndex}): send
     *       {@code TimeoutNow} immediately — the transferee starts its election</li>
     *   <li><b>Behind</b>: send {@code AppendEntries} to replicate the missing
     *       entries. When the transferee's {@code AppendEntriesResponse} confirms
     *       catch-up, {@link #handleSuccessfulAppend} sends the {@code TimeoutNow}</li>
     * </ul>
     *
     * <p>While a transfer is active, the leader blocks new proposals
     * (see {@code handleProposal}) to prevent the goal post from moving.
     * The transfer is aborted if the quorum check timer fires before
     * completion (transferee is unreachable or too slow).</p>
     *
     * @param l  the current leader role
     * @param tl the transfer request (carries the target {@code transferee})
     */
    private void handleLeadershipTransfer(Leader l, Message.TransferLeadership tl) throws StorageException {
        // Self-transfer: no-op.
        if (tl.transferee().equals(id)) {
            return;
        }

        var progress = l.progress(tl.transferee());

        // Unknown node or learner: only voters can become leader.
        if (progress == null || membership.isLearner(tl.transferee())) {
            return;
        }

        // Already transferring to this target: don't restart.
        if (l.isLeaderTransferee(tl.transferee())) {
            return;
        }

        l.transferLeadership(tl.transferee());

        if (progress.match() == log.lastIndex()) {
            // Transferee is caught up — tell it to start its election now.
            send(new Message.TimeoutNow(tl.transferee(), id, term));
        } else {
            // Transferee is behind — replicate missing entries first.
            trySendAppend(l, tl.transferee());
        }
    }

    /**
     * Handles a {@code TransferLeadership} request at a Follower.
     *
     * <p>Followers cannot process leadership transfers — only the leader can.
     * If a leader is known, the request is forwarded to it. If no leader is
     * known (e.g., during an election), the request is dropped because there
     * is no one to forward to.</p>
     *
     * @param f  the current follower role
     * @param tl the transfer request to forward
     */
    private void handleLeadershipTransfer(Follower f, Message.TransferLeadership tl) {
        if (!f.hasLeader()) {
            return;
        }
        send(new Message.TransferLeadership(f.leaderId(), id, tl.transferee(), tl.term()));
    }

    /**
     * Handles a {@code TransferLeadership} request at a Learner.
     *
     * <p>Same forwarding logic as for Followers — learners cannot process
     * transfers, so the request is forwarded to the known leader or dropped
     * if no leader is known.</p>
     *
     * @param l  the current learner role
     * @param tl the transfer request to forward
     */
    private void handleLeadershipTransfer(Learner l, Message.TransferLeadership tl) {
        if (!l.hasLeader()) {
            return;
        }
        send(new Message.TransferLeadership(l.leaderId(), id, tl.transferee(), tl.term()));
    }


}


























