package consensus.algorithm;

import consensus.membership.JointConfig;
import consensus.membership.MembershipConfig;
import consensus.membership.VoteResult;
import consensus.message.Message;
import consensus.node.NodeId;
import consensus.node.ReadState;
import consensus.storage.Entry;
import consensus.storage.RaftLog;
import consensus.storage.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
    private final List<ReadState> readStates;

    private Role role;

    public Raft(NodeId id, RaftLog log, int electionTimeout, int heartbeatTimeout, ElectionProtocol protocol, boolean checkQuorum, ProposalHandleMode proposalHandleMode, long maxMsgSize, long maxUncommittedSize, int maxInflightMsgs, long maxInflightBytes, List<Message> messages, List<Message> messagesAfterAppend, List<ReadState> readStates) {
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
        var follower = new Follower(leader);
        // Only clear votedFor on term change — preserves the
        // "one vote per term" invariant on same-term transitions.
        if (nextTerm != term) {
            term = nextTerm;
            votedFor = Optional.empty();
        }
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
    private Leader becomeLeader() {
        if (role instanceof Follower)
            throw new IllegalStateException("Cannot transition from Follower role to Leader role");

        var peers = membership.allReplicationTargets();
        var leader = new Leader(id, peers, new Inflight.Config(maxInflightMsgs, maxInflightBytes));
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
    private Learner becomeLearner(long nextTerm) {
        var learner = new Learner();
        // Same logic as becomeFollower — only clear votedFor on term change.
        if (nextTerm != term) {
            term = nextTerm;
            votedFor = Optional.empty();
        }
        role = learner;
        return learner;
    }


    // MESSAGE HANDLING PER ROLE
    private void handleMessage(Leader l, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse res-> {
                // Majority votes already gained and transitioned to Candidate -> Leader
                // Late response from some Voter for PreCandidate request
                // sent during PreElection. No need to handle stale messages.
            }
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.RequestVoteResponse res -> {
                // Majority votes already gained and transitioned to Candidate -> Leader
                // Late response from some Voter for Candidate request
                // sent during Election. No need to handle stale messages.
            }
            default -> {}
        }
    }

    private void handleMessage(Candidate c, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse res ->  {
                // Majority votes already gained and transitioned to Candidate.
                // Late response from some voter for PreCandidate request
                // sent during PreElection. No need to handle stale messages.
            }
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.RequestVoteResponse res -> handleVoteResponse(c, res);
            default -> {}
        }
    }

    private void handleMessage(Follower f, Message m) throws StorageException {
        switch (m) {
            case Message.TriggerElection(_) -> startElection(f);
            case Message.RequestPreVote preVote -> handlePreVoteReq(f, preVote);
            case Message.RequestPreVoteResponse res -> {
                // To handle stale response for RequestPreVote sent by Candidate
                // which is now Follower because Candidate found someone at
                // higher term so transitioned to Follower
            }
            case Message.RequestVote voteReq -> handleVoteReq(f, voteReq);
            case Message.RequestVoteResponse res -> {
                // To handle stale response for RequestVote sent by Candidate
                // which is now Follower because Candidate found someone at
                // higher term so transitioned to Follower
            }
            default -> {}
        }
    }

    private void handleMessage(Learner l, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> handlePreVoteReq(l, preVote);
            case Message.RequestVote voteReq -> handleVoteReq(l, voteReq);
            default -> {}
        }
    }

    private void handleMessage(PreCandidate pc, Message m) throws StorageException {
        switch (m) {
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse res -> handlePreVoteResponse(pc, res);
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            default -> {}
        }
    }


    // TICK HANDLING PER ROLE

    private void handleTick(Leader l) throws StorageException {
        if (l.canCheckQuorumAfterTick()) {
            step(new Message.CheckQuorum(id));
            if (l.equals(role)) {
                l.abortLeaderTransfer();
            }
        }

        if (!l.equals(role))
            return;

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
            candidateElection();
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

        /* Would-be term: what our term would be if we proceed to a real election.
         * Advertised in PreVote so voters can evaluate it, but not adopted yet. */
        var newTerm = term + 1;

        for (var voter: membership.voters().allVoters()) {
            /* Self-vote routed through send() so it lands in messagesAfterAppend
             * and is only processed after persistence. */
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
     * for self) and sends RequestVote to all voters.
     *
     * The self-vote is sent as a RequestVoteResponse through send(), which queues
     * it in messagesAfterAppend — ensuring the term and vote are persisted before
     * the vote is counted.
     */
    private void candidateElection() throws StorageException {
        becomeCandidate();
        for (var voter: membership.voters().allVoters()) {
            // Self-vote routed through send() so it lands in messagesAfterAppend
            // and is only processed after persistence.
            if (voter.equals(id)) {
                send(new Message.RequestVoteResponse(id, id, term, true));
                continue;
            }

            var lastEntry = log.lastEntryId();
            send(new Message.RequestVote(voter, id, term, lastEntry.term(), lastEntry.index()));
        }
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
     * Handles a PreVote request received while in the Follower role.
     *
     * @param f        the current follower role
     * @param preVote  the incoming PreVote request
     */
    private void handlePreVoteReq(Follower f, Message.RequestPreVote preVote) throws StorageException {
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
            case VoteResult.WON -> candidateElection();
            // Step down at our CURRENT term, not the response's term. The response
            // carries the would-be future term — using it would inflate our term,
            // defeating the purpose of PreVote.
            case VoteResult.LOST -> becomeFollower(term);
            case VoteResult.PENDING -> {}
        }
    }


    /**
     * Handles a RequestVote received by Leader, Candidate, or PreCandidate.
     *
     * If the vote is for a higher term, steps down to follower first (adopting
     * the new term and clearing vote/leader state) then evaluates as a follower.
     * Same or lower term votes are rejected — these roles have already voted
     * for themselves or are otherwise committed.
     *
     * @param voteReq the incoming vote request
     */
    private void handleHigherTermVoteReq(Message.RequestVote voteReq) throws StorageException {
        // Higher term: step down to follower first, then evaluate the vote
        // as a plain follower — becomeFollower clears leader/vote state.
        if (voteReq.term() > term) {
            handleVoteReq(becomeFollower(voteReq.term()), voteReq);
            return;
        }
        // Same or lower term: reject outright — as Leader/Candidate/PreCandidate
        // we have already voted for ourselves or are otherwise committed.
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, false));
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
        var voteGranted = canGrantVote(voteReq.from(), voteReq.lastLogTerm(), voteReq.lastLogIndex(), l.hasLeader());
        if (voteGranted) {
            votedFor = Optional.of(voteReq.from());
        }
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, voteGranted));
    }

    /**
     * Handles a RequestVote received while in the Follower role.
     *
     * @param f        the current follower role
     * @param voteReq  the incoming vote request
     */
    private void handleVoteReq(Follower f, Message.RequestVote voteReq) throws StorageException {
        // Higher term: adopt it first. becomeFollower clears votedFor and leader,
        // ensuring canGrantVote runs with a clean slate at the new term.
        // The term must be adopted even if the vote is ultimately rejected (stale log).
        if (voteReq.term() > term) {
            f = becomeFollower(voteReq.term());
        }
        var voteGranted = canGrantVote(voteReq.from(), voteReq.lastLogTerm(), voteReq.lastLogIndex(), f.hasLeader());

        if (voteGranted) {
            // Reset election timer — granting a vote shows the cluster is actively
            // electing, so don't start our own election right away.
            // Also clears leader since we're endorsing a new candidate.
            f.voteGranted();
            votedFor = Optional.of(voteReq.from());
        }
        send(new Message.RequestVoteResponse(voteReq.from(), id, term, voteGranted));
    }

    /**
     * Core vote eligibility check for real elections. Determines whether this node
     * can grant its vote to the requesting candidate.
     *
     * Invariant: term == voteReq.term() by the time this method is called.
     * Callers must adopt any higher term (via becomeFollower/becomeLearner) before
     * invoking this, so the decision is always made at the correct term.
     *
     * A vote is granted when BOTH conditions hold:
     *   1. The node is eligible to vote for the sender:
     *      - Either: hasn't voted yet AND no known leader (leader lease protection)
     *      - Or: already voted for this same sender (idempotent re-grant)
     *   2. The sender's log is at least as up-to-date as ours
     *
     * @param from          the candidate requesting the vote
     * @param lastLogTerm   the candidate's last log entry term
     * @param lastLogIndex  the candidate's last log entry index
     * @param hasLeader     whether this node currently knows of a leader
     * @return true if the vote can be granted
     */
    private boolean canGrantVote(NodeId from, long lastLogTerm, long lastLogIndex, boolean hasLeader) throws StorageException {
        var hasVoteToCast = votedFor.isEmpty();
        // Haven't voted for anyone and no known leader. If a leader exists, granting
        // would disrupt a healthy cluster (leader lease protection).
        var hasVoteToCastAndNoLeader = hasVoteToCast && !hasLeader;

        // We already voted for this same node — idempotent, safe to grant again.
        var alreadyVotedToSender = !hasVoteToCast && votedFor.get().equals(from);

        var isSenderLogUpToDate = log.isUpToDate(lastLogTerm, lastLogIndex);

        return (hasVoteToCastAndNoLeader || alreadyVotedToSender) && isSenderLogUpToDate;
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



    // Log replication
    private boolean appendEntries(Leader l, List<Entry> entries) throws StorageException {
        if (l.exceedUncommittedSize(entries)) {
            return false;
        }
        var lastIndex = log.append(entries);
        send(new Message.AppendEntriesResponse(id, id, term, true, lastIndex, 0, 0));
        return true;
    }

    private boolean appendProposalEntries(Leader l, List<Entry> entries) throws StorageException {
        var nextIndex = log.lastIndex() + 1;
        var normalEntries = new ArrayList<Entry>(entries.size());
        for (var entry: entries) {
            normalEntries.add(Entry.normal(term, nextIndex++, entry.data()));
        }
        return appendEntries(l, normalEntries);
    }

    private boolean appendPlaceholderEntry(Leader l) throws StorageException {
        return appendEntries(l, List.of(Entry.placeholder(term, log.lastIndex() + 1)));
    }

}
