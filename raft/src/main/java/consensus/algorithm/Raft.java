package consensus.algorithm;

import consensus.config.RaftConfig;
import consensus.membership.*;
import consensus.message.Message;
import consensus.engine.CheckpointState;
import consensus.engine.PersistentState;
import consensus.engine.VolatileState;
import consensus.storage.*;

import java.lang.reflect.Member;
import java.util.*;
import java.util.function.Function;
import java.util.random.RandomGenerator;

public class Raft {
    private final NodeId id;
        private long term;
    private Optional<NodeId> votedFor;

    private final RaftConfig config;
    private final RaftLog log;
    private MembershipConfig membership;
    
    // Role - Leader, Follower, Learner, Candidate, PreCandidate
    private Role role;

    private final RandomGenerator random;

    private Rejection rejection;

    // Output buffers
    private final List<Message.Peer> messages;
    private final List<Message.Peer> messagesAfterAppend;
    private final List<ReadState> readStates;
    private final Queue<ReadState> readsAwaitingApply;

    public Raft(
            RaftState state,
            RaftConfig raftConfig,
            LogStorage logStorage,
            MembershipConfig membershipConfig,
            RandomGenerator randomGenerator
    ) throws StorageException {
        id = state.id();
        term = state.term();
        votedFor = state.votedFor();
        config = raftConfig;
        log = new RaftLog(logStorage, config);
        random = randomGenerator;
        role = new Follower(config, random);
        membership = membershipConfig;
        messages = new ArrayList<>();
        messagesAfterAppend = new ArrayList<>();
        readStates = new ArrayList<>();
        readsAwaitingApply = new ArrayDeque<>();
        rejection = null;
    }

    // ────────────────────── APIS ──────────────────────
    public Optional<Rejection> step(Message m) throws StorageException {
        rejection = null;
        switch (role) {
            case Leader l -> handleMessage(l, m);
            case Candidate c -> handleMessage(c, m);
            case Follower f -> handleMessage(f, m);
            case Learner ln -> handleMessage(ln, m);
            case PreCandidate pc -> handleMessage(pc, m);
        }
        return Optional.ofNullable(rejection);
    }

    public NodeId id() {
        return id;
    }

    public long term() {
        return term;
    }

    public boolean hasOutput() {
        return !messages.isEmpty() || !messagesAfterAppend.isEmpty() || !readStates.isEmpty();
    }


    private <T> List<T> drain(List<T> list) {
        var items = new ArrayList<>(list);
        list.clear();
        return items;
    }

    public List<ReadState> readsAwaitingApply() {
        return List.copyOf(readsAwaitingApply);
    }

    public List<Message.Peer> drainMessages() {
        return drain(messages);
    }

    public List<Message.Peer> drainMessagesAfterAppend() {
        return drain(messagesAfterAppend);
    }

    public List<ReadState> drainReadStates() {
        return drain(readStates);
    }

    public PersistentState persistentState() {
        return new PersistentState(term, votedFor);
    }

    public VolatileState volatileState() {
        var leaderId = switch (role) {
            case Leader _ -> id;
            case Follower f when f.hasLeader() -> f.leaderId();
            case Learner l when l.hasLeader() -> l.leaderId();
            default -> null;
        };
        return new VolatileState(role.type(), Optional.ofNullable(leaderId));
    }

    public long commitIndex() {
        return log.committed();
    }

    public long appliedIndex() {
        return log.applied();
    }

    public MembershipConfig membership() {
        return membership;
    }

    public Optional<Map<NodeId, PeerProgress>> peerProgress() {
        if (role instanceof Leader l) {
            return Optional.of(l.peerProgress());
        }

        return Optional.empty();
    }

    public Optional<NodeId> leaderTransferee() {
        if (role instanceof Leader l) {
           return l.transferTarget();
        }

        return Optional.empty();
    }

    public CheckpointState checkpointState() {
        return new CheckpointState(log.committed());
    }

    public RaftLog log() {
        return log;
    }

    // ────────────────────── SENDING MESSAGES ──────────────────────
    private void send(Message.Peer m) {
        if (m instanceof Message.AppendEntriesResponse || m instanceof Message.RequestPreVoteResponse || m instanceof Message.RequestVoteResponse) {
            messagesAfterAppend.add(m);
        } else {
            messages.add(m);
        }

    }

    private void reject(Rejection r) {
        rejection = r;
    }

    private void reject(DataDropReason reason) {
        reject(new Rejection.DataProposalRejected(reason));
    }

    private void reject(MembershipChangeDropReason reason) {
        reject(new Rejection.MembershipChangeRejected(reason));
    }

    private void reject(ReadDropReason reason) {
        reject(new Rejection.ReadIndexRejected(reason));
    }

    // ────────────────────── ROLE TRANSITIONS ──────────────────────

    /**
     * Transitions to PreCandidate — a transient role that probes whether
     * this node could win an election without actually incrementing the term.
     *
     * <p>Unlike becomeCandidate, this does NOT change term or votedFor.
     * That's the whole point of PreVote — if the node is partitioned and
     * can't win, it hasn't disrupted the cluster with a term bump.</p>
     *
     * <p>Valid: Follower → PreCandidate<br>
     * Invalid: Leader, PreCandidate → PreCandidate (throws)</p>
     *
     * @return the newly created PreCandidate role instance
     */
    private PreCandidate becomePreCandidate() {
        if (role instanceof Leader)
            throw new IllegalStateException("Cannot transition from " + role + " to PreCandidate role");

        if (config.electionProtocol() != ElectionProtocol.DUAL_ELECTION)
            throw new IllegalStateException("Cannot transition to PreCandidate role in " + config.electionProtocol() + " election mode");

        var preCandidate = new PreCandidate(config, random);
        role = preCandidate;
        return preCandidate;
    }

    /**
     * Transitions to Candidate — starts a real election by incrementing
     * the term and recording a self-vote. Unlike becomePreCandidate, this
     * is a commitment: even if the election fails, the term has been bumped.
     *
     * <p>Valid: Follower, PreCandidate → Candidate<br>
     * Invalid: Leader, Candidate → Candidate (throws)</p>
     *
     * @return the newly created Candidate role instance
     */
    private Candidate becomeCandidate() {
        if (role instanceof Leader)
            throw new IllegalStateException("Cannot transition from " + role + " to Candidate role");

        term = term + 1;
        votedFor = Optional.of(id);
        var candidate = new Candidate(config, random);
        role = candidate;
        return candidate;
    }

    /**
     * Transitions to Follower, optionally with a known leader.
     *
     * <p>This is the most common transition — it happens when:</p>
     * <ul>
     *   <li>A higher-term message arrives (Leader/Candidate/PreCandidate
     *       steps down)</li>
     *   <li>An election is lost (Candidate/PreCandidate falls back)</li>
     *   <li>An AppendEntries or heartbeat from a new leader is accepted</li>
     * </ul>
     *
     * <p>votedFor is only cleared when the term actually changes. If
     * nextTerm == term, the vote is preserved — clearing it would violate
     * "vote at most once per term" and could allow a node to double-vote
     * within the same term.</p>
     *
     * @param nextTerm the term to adopt (may be same as current)
     * @param leader   the known leader, or null if unknown
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
        var follower = new Follower(leader, config, random);

        role = follower;
        return follower;
    }

    /**
     * Convenience overload — transitions to Follower with no known leader.
     * Used when stepping down due to a higher-term vote request or election loss,
     * where the leader identity is not yet known.
     *
     * @param nextTerm the term to adopt
     * @return the newly created Follower role instance
     */
    private Follower becomeFollower(long nextTerm) {
        return becomeFollower(nextTerm, null);
    }

    /**
     * Transitions to Leader after winning an election.
     *
     * <p>Initializes replication state (PeerProgress) for every peer in
     * the cluster, setting each peer's nextIndex to our last log
     * index + 1.</p>
     *
     * <p>Valid: Candidate → Leader<br>
     * Invalid: Follower → Leader (throws — must win an election first)</p>
     *
     * <p>Note: the caller is responsible for appending the initial
     * placeholder entry after this method returns. The placeholder is
     * needed so the new leader can commit entries from prior terms
     * (Raft §5.4.2 — a leader can only commit entries from its own term,
     * and the no-op entry serves that purpose).</p>
     *
     * @return the newly created Leader role instance
     */
    private Leader becomeLeader() throws StorageException {
        if (!(role instanceof Candidate))
            throw new IllegalStateException("Cannot transition from " + role.type() + " role to Leader role");

        var peers = membership.members();
        var leader = new Leader(id, peers, log.lastIndex(), config);
        role = leader;
        return leader;
    }
    /**
     * Transitions to Learner, adopting a new term if higher.
     *
     * <p>Mirrors becomeFollower's term/vote logic — votedFor is cleared
     * only on term change to preserve the "one vote per term" invariant.</p>
     *
     * <p>This is used when a Learner receives a message with a higher
     * term. Learners don't participate in elections or become candidates,
     * but they still need to track the current term and vote state
     * correctly so they can respond to RequestVote and RequestPreVote
     * (see {@link #handleVoteReq(Learner, Message.RequestVote)}).</p>
     *
     * @param nextTerm the term to adopt (may be same as current)
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

        var learner = new Learner(leader, config);

        // Same logic as becomeFollower — only clear votedFor on term change.
        role = learner;
        return learner;
    }

    private Learner becomeLearner(long nextTerm) {
        return becomeLearner(nextTerm, null);
    }

    // ────────────────────── TICK HANDLING PER ROLE ──────────────────────

    /**
     * Handles a tick for the Leader role. The leader has two periodic timers:
     *
     * <ol>
     *   <li><b>Quorum check (election timeout interval):</b> Verifies that
     *       a majority of peers have responded recently. If quorum is lost,
     *       the leader steps down to Follower to avoid a "zombie leader"
     *       that is partitioned from the cluster. After a quorum check, any
     *       in-progress leadership transfer is aborted — if the transfer
     *       hasn't completed within an election timeout, the transferee is
     *       likely unreachable.</li>
     *   <li><b>Heartbeat (heartbeat timeout interval):</b> Broadcasts
     *       heartbeats to all peers to maintain leader authority and prevent
     *       followers from starting elections. Only fires if the node is
     *       still a leader (the quorum check above may have caused a
     *       step-down).</li>
     * </ol>
     *
     * <p>Both timers are dispatched through {@code step()} as internal
     * messages ({@link Message.CheckQuorum},
     * {@link Message.TriggerHeartbeat}) to reuse the standard message
     * handling pipeline.</p>
     *
     * <h4>Future improvement 1: Soft Quorum + Vote Rejection for Lease Reads</h4>
     *
     * <p>Currently, checkQuorum does three things at once:</p>
     * <ol>
     *   <li>Leader steps down if it loses majority contact (partition
     *       detection)</li>
     *   <li>Followers reject votes from higher-term nodes if a leader was
     *       recently heard from (prevents disruptive elections from
     *       partitioned nodes)</li>
     *   <li>Together (1) + (2) form the implicit lease window for
     *       lease-based reads</li>
     * </ol>
     *
     * <p>The step-down in (1) is aggressive: a brief network hiccup causes
     * the leader to abdicate even though it may still be the rightful
     * leader. For lease reads, all we actually need is:</p>
     * <ul>
     *   <li><b>Soft quorum check:</b> the leader tracks whether it has
     *       heard from a majority recently. If not, it stops serving lease
     *       reads but does NOT step down. It can still serve heartbeat-based
     *       reads and accept writes.</li>
     *   <li><b>Vote rejection on followers:</b> followers still reject
     *       RequestVote from higher-term nodes if the leader was recently
     *       heard from. This prevents a partitioned node from forcing an
     *       election, which is what actually invalidates the lease.</li>
     * </ul>
     *
     * <p>With soft quorum + vote rejection, the lease guarantee is preserved
     * (no other leader can be elected while followers believe the current
     * leader is alive), but the leader doesn't step down unnecessarily.
     * This decouples "lease validity" from "leader liveness", giving better
     * availability.</p>
     *
     * <h4>Future improvement 2: Sliding Window Heartbeat Liveness</h4>
     *
     * <p>The current liveness check uses a fixed-window reset cycle:</p>
     *
     * <pre>
     *   election timeout fires -> collect active flags -> reset all to inactive
     *                          -> next cycle starts
     *
     *   Problem (fixed window boundary):
     *     cycle N                          cycle N+1
     *     |--- HB sent --- HB sent ---|--- HB sent --- HB sent ---|
     *                            ↑
     *                      delayed HB response from cycle N-1 arrives
     *                      -> marks peer as active in cycle N
     *                      -> peer counted as alive even though it hasn't
     *                         responded to any HB in this cycle
     * </pre>
     *
     * <p>A sliding-window approach using heartbeat seq from
     * {@link ReadIndex} avoids this boundary artifact:</p>
     *
     * <pre>
     *   Instead of boolean active flags, check:
     *     peerAckedSeq[peer] >= currentSeq - N
     *
     *   where N = heartbeat rounds per eljection timeout (e.g., 10)
     *
     *   A peer is "alive" if it has acked a heartbeat within the last N rounds.
     *   No reset cycle, no boundary artifacts. Each ack is timestamped with its
     *   exact round number.
     * </pre>
     *
     * <p><b>Consideration: AppendEntries responses.</b></p>
     * <ul>
     *   <li>The current boolean active flag is set by ANY response from a
     *       peer — heartbeat responses, AppendEntries responses, etc.</li>
     *   <li>peerAckedSeq only tracks heartbeat acks, so AppendEntries
     *       responses wouldn't count as liveness signals</li>
     *   <li>If implementing, also update peerAckedSeq on AppendEntries
     *       responses:
     *       {@code peerAckedSeq[peer] = max(peerAckedSeq[peer], currentSeq)},
     *       treating any response as equivalent to acking the latest
     *       round</li>
     *   <li>In practice heartbeats and AppendEntries share the same network
     *       path, so if one gets through the other almost certainly will
     *       too</li>
     * </ul>
     *
     * <p>Why the current approach is acceptable despite this
     * imprecision:</p>
     * <ul>
     *   <li>Election timeout is long (e.g., 10 heartbeat intervals), so
     *       many heartbeats are sent per cycle</li>
     *   <li>A single stale response is unlikely to be the only signal — if
     *       a peer is truly alive, it will respond to current heartbeats
     *       too</li>
     *   <li>The window drift is at most one heartbeat interval, which is
     *       small relative to the election timeout</li>
     *   <li>Not theoretically tight, but practically safe for most
     *       deployments</li>
     * </ul>
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
        if (!l.equals(role)) return;

        // Heartbeat fires on heartbeat timeout interval — typically much shorter than
        // election timeout (e.g. 1 tick vs 10 ticks).
        if (l.canSendHeartBeatAfterTick()) {
            step(new Message.TriggerHeartbeat(id));
        }
    }

    /**
     * Handles a tick for the PreCandidate role. Same election round timer
     * logic as {@link #handleTick(Candidate)} — if the pre-vote round
     * stalls, the node retries with a fresh pre-election and new randomized
     * timeout.
     */
    private void handleTick(PreCandidate pc) throws StorageException {
        if (pc.electionRoundTimedOutAfterTick()) {
            step(new Message.TriggerElection(id));
        }
    }

    /**
     * Handles a tick for the Candidate role. A Candidate runs a single
     * timer — the election round timer — which bounds how long a real
     * election round can take before giving up and retrying.
     *
     * <p>If the round timer expires without receiving votes from a majority
     * (split vote, network partition, or unresponsive peers), the node
     * restarts the election with a fresh term increment and new randomized
     * timeout. The randomization across nodes ensures that repeated split
     * votes eventually resolve as one candidate's timer fires first.</p>
     *
     * <p>The retry is dispatched as a {@link Message.TriggerElection} through
     * {@code step()}, which calls {@code candidateElection()} with
     * {@link ElectionCause#ELECTION_ROUND_TIMEOUT} — incrementing the term,
     * recording a self-vote, and broadcasting {@code RequestVote} to all
     * voters.</p>
     */
    private void handleTick(Candidate c) throws StorageException {
        if (c.electionRoundTimedOutAfterTick()) {
            step(new Message.TriggerElection(id));
        }
    }

    /**
     * Handles a tick for the Follower role. A Follower runs a single timer —
     * the election timer — which detects leader failure and triggers a new
     * election.
     *
     * <p>Two conditions must be met before starting an election:</p>
     * <ol>
     *   <li><b>Eligible to participate:</b> the node must be a voting member
     *       of the current configuration (learners and non-members cannot
     *       start elections).</li>
     *   <li><b>Election timer expired:</b> the leader has not been heard from
     *       within the randomized election timeout, indicating it may have
     *       failed or become partitioned.</li>
     * </ol>
     *
     * <p>If both conditions hold, a {@link Message.TriggerElection} is
     * dispatched through {@code step()}, which initiates either a pre-vote
     * or a real election depending on the configured
     * {@link ElectionProtocol}.</p>
     */
    private void handleTick(Follower f) throws StorageException {
        if (canParticipateInElection(f) && f.canStartElectionAfterTick()) {
            step(new Message.TriggerElection(id));
        }
    }

    /**
     * Handles a tick for the Learner role. A Learner runs a single timer —
     * the lease timer — which tracks whether the known leader is still
     * alive.
     *
     * <p>Learners do not participate in elections and cannot start one, but
     * they still need to detect leader failure. If the lease timer expires
     * (no leader contact within the lease timeout), the learner forgets its
     * current leader. This ensures the learner does not indefinitely report
     * a stale leader to the application layer.</p>
     *
     * <p>The lease is renewed whenever the learner receives a message from
     * the leader (AppendEntries, heartbeat, snapshot). Unlike voters, a
     * learner takes no further action after forgetting the leader — it
     * simply waits for a new leader to contact it.</p>
     */
    private void handleTick(Learner l) {
        if (l.leaseExpiredAfterTick()) {
            l.forgetLeader();
        }
    }

    // ────────────────────── MESSAGE HANDLING PER ROLE ──────────────────────

    private void handleMessage(Leader l, Message m) throws StorageException {
        switch (m) {
            case Message.Tick _ -> handleTick(l);
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
            case Message.SnapshotStatus ss -> handleSnapshotStatus(l, ss);
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
            case Message.MembershipChangeProposal mcp -> handleMembershipChange(l, mcp);
            case Message.LeaveJointProposal _ -> handleLeaveJoint(l);
            case Message.ReadIndex ri -> handleReadIndex(l, ri);
            case Message.ReadIndexResponse _ -> {
                // Stale: this node was a Follower that forwarded a ReadIndex
                // to the old leader, then won an election and became Leader.
                // The old leader's response arrives late. Drop it — this node
                // now has its own ReadIndex tracking with fresh seq state.
                // The read context belongs to the previous term/role.
            }
            case Message.ForgetLeader _ -> {
                // No-op: a leader can't forget itself.
            }
            case Message.LogPersisted lp -> handleLogPersisted(l, lp);
            case Message.AppliedToStateMachine asm -> handleAppliedToStateMachine(l, asm);
            case Message.ApplyMembershipChange amc -> handleApplyMembershipChange(l, amc);
            case Message.ApplyLeaveJoint _ -> handleApplyLeaveJoint(l);
            default -> {
            }
        }
    }

    private void handleMessage(Candidate c, Message m) throws StorageException {
        switch (m) {
            case Message.Tick _ -> handleTick(c);
            case Message.TriggerElection(_) -> candidateElection(ElectionCause.ELECTION_ROUND_TIMEOUT);
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse _ -> {
                // Stale: this node was a PreCandidate that already won the
                // pre-election and transitioned to Candidate. Late
                // PreVoteResponses from the pre-election phase are irrelevant.
            }
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.RequestVoteResponse res -> handleVoteResponse(c, res);
            case Message.DataProposal _ -> reject(DataDropReason.NO_LEADER);
            case Message.MembershipChangeProposal _,
                 Message.LeaveJointProposal _ -> reject(MembershipChangeDropReason.NO_LEADER);
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
            case Message.ReadIndex _ -> {
                reject(ReadDropReason.NO_LEADER);
                // Dropped: a Candidate is mid-election with no known leader.
                // Cannot serve reads (not the leader) and cannot forward
                // (don't know who the leader is). The application should
                // retry after an election completes.
            }
            case Message.ReadIndexResponse _ -> {
                // Stale: this node was a Follower that forwarded a ReadIndex,
                // then started an election (became Candidate). The old leader's
                // response arrives late. Drop it — the node is mid-election
                // and doesn't serve client requests in this state. The read
                // context belongs to a previous role.
            }
            case Message.ForgetLeader _ -> {
                // No-op: a Candidate has no leader to forget (already
                // leaderless, mid-election).
            }
            case Message.LogPersisted lp -> handleLogPersisted(lp);
            case Message.AppliedToStateMachine asm -> handleAppliedToStateMachine(asm);
            case Message.ApplyMembershipChange amc -> handleApplyMembershipChange(amc);
            case Message.ApplyLeaveJoint _ -> handleApplyLeaveJoint();
            default -> {
            }
        }
    }

    private void handleMessage(PreCandidate pc, Message m) throws StorageException {
        switch (m) {
            case Message.Tick _ -> handleTick(pc);
            case Message.TriggerElection(_) -> preCandidateElection();
            case Message.RequestPreVote preVote -> rejectPreVote(preVote);
            case Message.RequestPreVoteResponse res -> handlePreVoteResponse(pc, res);
            case Message.RequestVote voteReq -> handleHigherTermVoteReq(voteReq);
            case Message.DataProposal _ -> reject(DataDropReason.NO_LEADER);
            case Message.MembershipChangeProposal _,
                 Message.LeaveJointProposal _ -> reject(MembershipChangeDropReason.NO_LEADER);
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
            case Message.ReadIndex _ -> {
                reject(ReadDropReason.NO_LEADER);
                // Dropped: a PreCandidate is mid-pre-election with no known
                // leader. Same reasoning as Candidate — cannot serve or
                // forward reads. The application should retry after an
                // election completes.
            }
            case Message.ReadIndexResponse _ -> {
                // Stale: this node was a Follower that forwarded a ReadIndex,
                // then started a pre-election (became PreCandidate). The old
                // leader's response arrives late. Drop it — same reasoning
                // as Candidate.
            }
            case Message.ForgetLeader _ -> {
                // No-op: a PreCandidate has no leader to forget (already
                // leaderless, mid-pre-election).
            }
            case Message.LogPersisted lp -> handleLogPersisted(lp);
            case Message.AppliedToStateMachine asm -> handleAppliedToStateMachine(asm);
            case Message.ApplyMembershipChange amc -> handleApplyMembershipChange(amc);
            case Message.ApplyLeaveJoint _ -> handleApplyLeaveJoint();
            default -> {
            }
        }
    }

    private void handleMessage(Follower f, Message m) throws StorageException {
        switch (m) {
            case Message.Tick _ -> handleTick(f);
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
            case Message.MembershipChangeProposal mcp -> handleMembershipChange(f, mcp);
            case Message.LeaveJointProposal _ -> handleLeaveJoint(f);
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
            case Message.ReadIndex ri -> handleReadIndex(f, ri);
            case Message.ReadIndexResponse rir -> handleReadIndexResponse(rir);
            case Message.ForgetLeader _ -> forgetLeader(f);
            case Message.LogPersisted lp -> handleLogPersisted(lp);
            case Message.AppliedToStateMachine asm -> handleAppliedToStateMachine(asm);
            case Message.ApplyMembershipChange amc -> handleApplyMembershipChange(f, amc);
            case Message.ApplyLeaveJoint _ -> handleApplyLeaveJoint(f);
            default -> {
            }
        }
    }

    private void handleMessage(Learner l, Message m) throws StorageException {
        switch (m) {
            case Message.Tick _ -> handleTick(l);
            case Message.RequestPreVote preVote -> handlePreVoteReq(l, preVote);
            case Message.RequestVote voteReq -> handleVoteReq(l, voteReq);
            case Message.DataProposal p -> handleProposal(l, p);
            case Message.MembershipChangeProposal mcp -> handleMembershipChange(l, mcp);
            case Message.LeaveJointProposal _ -> handleLeaveJoint(l);
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
            case Message.ReadIndex ri -> handleReadIndex(l, ri);
            case Message.ReadIndexResponse rir -> handleReadIndexResponse(rir);
            case Message.ForgetLeader _ -> forgetLeader(l);
            case Message.LogPersisted lp -> handleLogPersisted(lp);
            case Message.AppliedToStateMachine asm -> handleAppliedToStateMachine(asm);
            case Message.ApplyMembershipChange amc -> handleApplyMembershipChange(l, amc);
            case Message.ApplyLeaveJoint _ -> handleApplyLeaveJoint(l);
            default -> {
            }
        }
    }

    // ────────────────────── ELECTION HANDLING ──────────────────────

    /**
     * Checks whether this node is eligible to start an election.
     *
     * <p>Three conditions must hold:</p>
     * <ul>
     *   <li>The node is a voter — learners cannot campaign.</li>
     *   <li>No unstable snapshot is pending — the node must finish applying
     *       the snapshot before it can safely run an election.</li>
     *   <li>No unapplied membership changes — see
     *       {@link #hasUnappliedMembershipChange()}.</li>
     * </ul>
     */
    private boolean canParticipateInElection(Follower f) throws StorageException {
        return membership.isVoter(id) && !log.hasUnstableSnapshot() && !hasUnappliedMembershipChange();
    }

    /**
     * Returns {@code true} if there are committed but unapplied membership
     * change entries in the log.
     *
     * <p>This is a critical safety gate before elections. A node must apply
     * all committed config changes before campaigning. Without this check,
     * a node can win an election using a stale quorum, leading to entries
     * committed without true majority or even committed entries being
     * lost.</p>
     *
     * <h4>Example 1 — committed without true majority (3 → 4 nodes)</h4>
     * <ol>
     *   <li>Cluster: {A (leader), B (follower), C (follower)}.
     *       Quorum = 2 of 3.</li>
     *   <li>A proposes "add D". Replicated to A, B, C → committed.
     *       Committed config = {A, B, C, D}, quorum = 3 of 4.</li>
     *   <li>A applies it → A's active config = {A, B, C, D}.
     *       D joins, catches up.</li>
     *   <li>B: entry committed but unapplied → active config still
     *       {A, B, C}. C may or may not have applied it — doesn't
     *       matter (vote granting does not check the candidate's
     *       membership, so C votes for B regardless of C's own
     *       config).</li>
     *   <li>A becomes unreachable.</li>
     *   <li>B's election timer fires. B campaigns with stale config
     *       {A, B, C}. C votes for B → 2 of 3 → B becomes leader.</li>
     *   <li>Client write arrives. B replicates. B + C acknowledge →
     *       committed under {A, B, C} (2 of 3).</li>
     *   <li>Real config is {A, B, C, D}, quorum = 3 of 4. The write
     *       was only acknowledged by 2 nodes — <b>committed without
     *       true majority</b>.</li>
     * </ol>
     *
     * <h4>Example 2 — committed entry lost (3 → 5 nodes)</h4>
     * <ol>
     *   <li>Cluster: {A (leader), B (follower), C (follower)}.
     *       Quorum = 2 of 3.</li>
     *   <li>A proposes "add D" (entry 5). Replicated to A, B, C →
     *       committed. A applies it → A's config = {A, B, C, D}.
     *       D joins, catches up.</li>
     *   <li>A proposes "add E" (entry 6). Replicated to A, B, C, D →
     *       committed. A applies it → A's config = {A, B, C, D, E}.
     *       E joins, catches up.</li>
     *   <li>A proposes data entry 7. Replicated to D and E
     *       (B, C are slow). {A, D, E} = 3 of 5 →
     *       <b>entry 7 is committed</b>.</li>
     *   <li>Partition: {A, D, E} vs {B, C}.</li>
     *   <li>B has entries 1–6 committed, 1–3 applied. Entry 4 (data)
     *       is slow to apply → entries 5, 6 (config changes) queued
     *       behind it. B's active config = {A, B, C},
     *       quorum = 2 of 3.</li>
     *   <li>B campaigns with {A, B, C}. Votes from B + C → 2 of 3 →
     *       B becomes leader (term 2). B does NOT have entry 7.</li>
     *   <li>B proposes entry 7' (different data, term 2).
     *       B + C acknowledge → committed under stale config.</li>
     *   <li>Partition heals. A sees term 2, steps down.</li>
     *   <li>B sends AppendEntries → A, D, E truncate entry 7 (term 1),
     *       accept entry 7' (term 2). <b>Committed entry lost</b>.</li>
     * </ol>
     *
     * <p>Root cause for both: the election quorum (stale config) and the
     * commit quorum (real config) can have <b>zero overlap</b>, breaking
     * Raft's fundamental guarantee that every election quorum intersects
     * every commit quorum.</p>
     *
     * <p>This check prevents both scenarios. By forcing the node to apply
     * all committed config changes before campaigning, it uses the correct
     * config and requires the correct quorum size for the election.</p>
     *
     * <p>Scans entries in the range {@code (applied, committed]} looking
     * for any {@link Entry.MembershipChange} or
     * {@link Entry.LeaveJoint}. Returns early on the first match.</p>
     */
    private boolean hasUnappliedMembershipChange() throws StorageException {
        if (log.applied() >= log.committed()) {
            return false;
        }

        var it = log.iterator(log.applied() + 1, log.committed() + 1, Long.MAX_VALUE);
        while (it.hasNext()) {
            var entry = it.next();
            if (entry instanceof Entry.MembershipChange || entry instanceof Entry.LeaveJoint) {
                return true;
            }
        }
        return false;
    }

    /**
     * Entry point when a follower's election timeout fires.
     * Routes to pre-election or real election based on the configured protocol.
     *
     * @param f the follower whose election timeout triggered this
     */
    private void startElection(Follower f) throws StorageException {
        if (config.electionProtocol() == ElectionProtocol.DUAL_ELECTION) {
            preCandidateElection();
        } else {
            candidateElection(ElectionCause.ELECTION_TIMEOUT);
        }
    }

    /**
     * Starts a pre-election: transitions to PreCandidate and sends
     * RequestPreVote to all voters with a would-be term (current + 1)
     * without actually incrementing it.
     *
     * <p>The self-vote is sent as a RequestPreVoteResponse through
     * send(), which queues it in messagesAfterAppend — ensuring the role
     * transition is persisted before the vote is counted.</p>
     */
    private void preCandidateElection() throws StorageException {
        becomePreCandidate();

        // Would-be term: what our term would be if we proceed to a real election.
        // Advertised in PreVote so voters can evaluate it, but not adopted yet.
        var newTerm = term + 1;

        for (var voter : membership.voters().allVoters()) {
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
     * Starts a real election: transitions to Candidate (incrementing term,
     * voting for self) and sends {@code RequestVote} to all voters.
     *
     * <p>The {@code cause} is embedded in every {@code RequestVote}
     * message, allowing recipients to distinguish between:</p>
     * <ul>
     *   <li>{@link ElectionCause#ELECTION_TIMEOUT} — normal election after timeout;
     *       voters with an active leader lease will reject</li>
     *   <li>{@link ElectionCause#LEADER_TRANSFER} — leadership transfer;
     *       voters bypass the leader lease check (the current leader
     *       authorized this)</li>
     *   <li>{@link ElectionCause#WON_PREELECTION} — won a pre-vote;
     *       proceeds to real election at the same term voters already
     *       indicated willingness to support</li>
     * </ul>
     *
     * <p>The self-vote is sent as a {@code RequestVoteResponse} through
     * {@link #send}, which queues it in {@code messagesAfterAppend} —
     * ensuring the term and vote are persisted before the vote is
     * counted.</p>
     *
     * @param cause why this election was initiated (carried on every vote request)
     */
    private void candidateElection(ElectionCause cause) throws StorageException {
        becomeCandidate();
        for (var voter : membership.voters().allVoters()) {
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
     * Starts an election triggered by a {@code TimeoutNow} message from
     * the current leader — a graceful leadership transfer.
     *
     * <p>Always uses a real election (never pre-vote), because the leader
     * already verified the transferee is caught up. The
     * {@link ElectionCause#LEADER_TRANSFER} cause tells voters to bypass
     * the leader lease check.</p>
     *
     * @param f the follower role that received the {@code TimeoutNow}
     */
    private void transferElection(Follower f) throws StorageException {
        candidateElection(ElectionCause.LEADER_TRANSFER);
    }

    /**
     * Handles a PreVote request received while in the Learner role.
     *
     * <p>A learner's committed config may be stale — the cluster could
     * have already committed a config change promoting this learner to
     * voter, but the learner hasn't received it yet. If the learner
     * refuses to vote during that window, the remaining voters may not
     * form a quorum, deadlocking the cluster. So learners always
     * participate when asked to vote.</p>
     *
     * <p>Example:</p>
     * <ol>
     *   <li>Cluster has A(learner), B(voter/leader), C(voter)</li>
     *   <li>A config change promoting A to voter is committed on
     *       quorum {B, C}</li>
     *   <li>A hasn't received this config change yet — still thinks
     *       it's a learner</li>
     *   <li>B (the leader) crashes</li>
     *   <li>New voter set is {A, B, C}, quorum requires 2 votes</li>
     *   <li>C votes for itself — 1 vote. B is dead — unreachable</li>
     *   <li>C sends RequestPreVote to A</li>
     *   <li>If A refuses (thinks it's a learner) → no quorum possible
     *       → cluster deadlocks</li>
     *   <li>If A votes → C wins → becomes leader → replicates config
     *       change to A → A finally learns it's a voter</li>
     * </ol>
     *
     * @param l       the current learner role
     * @param preVote the incoming PreVote request
     */
    private void handlePreVoteReq(Learner l, Message.RequestPreVote preVote) throws StorageException {
        handlePreVoteReq(preVote, l.hasLeader());
    }

    /**
     * Handles a {@code RequestPreVote} received while in the Follower role.
     *
     * <p><b>Follower lease protection (PreVote):</b> if all of the
     * following hold, the pre-vote is explicitly rejected (not silently
     * ignored — unlike {@code RequestVote}, pre-votes don't carry a term
     * that we must adopt, so a rejection with our term lets the
     * pre-candidate learn about it):</p>
     * <ol>
     *   <li>The pre-vote is for a higher term</li>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The follower knows a leader ({@code hasLeader})</li>
     *   <li>The election timer has not reached the base election
     *       timeout</li>
     * </ol>
     *
     * <p>No {@link ElectionCause} bypass here — transfer elections always
     * use real {@code RequestVote} (never pre-vote), so a pre-vote is
     * never a transfer election.</p>
     *
     * @param f       the current follower role
     * @param preVote the incoming PreVote request
     */
    private void handlePreVoteReq(Follower f, Message.RequestPreVote preVote) throws StorageException {
        // Lease check: recently heard from a leader → reject disruptive pre-vote.
        if (preVote.term() > term && config.leaderLivenessPolicy() == LeaderLivenessPolicy.QUORUM_VERIFIED && f.hasLeader() && !f.isElectionTimedOut()) {
            send(new Message.RequestPreVoteResponse(preVote.from(), id, term, false));
            return;
        }
        handlePreVoteReq(preVote, f.hasLeader());
    }

    /**
     * Core PreVote evaluation. A PreVote is granted when the node is eligible to
     * vote for the sender AND the sender's log is at least as up-to-date as ours.
     *
     * @param preVote   the incoming PreVote request
     * @param hasLeader whether this node currently knows of a leader (lease check)
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
     * Unconditionally rejects a PreVote. Used by Leader, Candidate, and
     * PreCandidate
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
     * @param pc  the current PreCandidate role holding the vote tally
     * @param res the incoming PreVote response
     */
    private void handlePreVoteResponse(PreCandidate pc, Message.RequestPreVoteResponse res) throws StorageException {
        pc.recordVote(res.from(), res.voteGranted());
        var result = membership.voteResult(pc.voteQuery());
        switch (result) {
            // Proceed to real election — candidateElection() increments term,
            // records self-vote, and sends RequestVote to all voters.
            case VoteResult.WON -> candidateElection(ElectionCause.WON_PREELECTION);
            // Step down at our CURRENT term, not the response's term. The response
            // carries the would-be future term — using it would inflate our term,
            // defeating the purpose of PreVote.
            case VoteResult.LOST -> becomeFollower(term);
            case VoteResult.PENDING -> {
            }
        }
    }

    /**
     * Handles a {@code RequestVote} received by Candidate or PreCandidate.
     *
     * <p>If the vote is for a higher term, steps down to Follower first
     * (adopting the new term, clearing vote and leader state) then
     * evaluates as a Follower via
     * {@link #handleVoteReq(Follower, Message.RequestVote)}. Same or
     * lower term votes are rejected outright — these roles have already
     * voted for themselves or are otherwise committed to their current
     * election.</p>
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
     * <p><b>Leader lease protection:</b> if all of the following hold,
     * the vote is silently ignored — the leader does not step down, does
     * not adopt the higher term, and sends no response:</p>
     * <ol>
     *   <li>The vote is for a higher term (a same/lower-term vote is
     *       simply rejected by {@link #handleHigherTermVoteReq})</li>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The quorum check timer has not elapsed — meaning the leader
     *       recently verified it still has a responsive majority</li>
     *   <li>The election is not a
     *       {@link ElectionCause#LEADER_TRANSFER} (the leader itself
     *       authorized that election)</li>
     * </ol>
     *
     * <p>Why silence instead of a rejection: the candidate is at a higher
     * term. Any response we send carries our lower term, which the
     * candidate would drop (lower-term responses are ignored). So
     * responding is pointless — the candidate will simply time out and
     * retry if it doesn't get enough votes.</p>
     *
     * <p>If the lease check does not apply (lease expired, checkQuorum
     * off, or transfer election), this falls through to
     * {@link #handleHigherTermVoteReq} which steps down to Follower and
     * evaluates the vote normally.</p>
     *
     * @param l       the current leader role (provides the quorum check timer)
     * @param voteReq the incoming vote request
     */
    private void handleVoteReq(Leader l, Message.RequestVote voteReq) throws StorageException {
        // Leader lease: recently verified quorum → reject disruptive votes.
        if (voteReq.term() > term && config.leaderLivenessPolicy() == LeaderLivenessPolicy.QUORUM_VERIFIED && !l.isQuorumCheckTimedOut() && voteReq.cause() != ElectionCause.LEADER_TRANSFER) {
            return;
        }
        handleHigherTermVoteReq(voteReq);
    }

    /**
     * Handles a RequestVote received while in the Learner role.
     * Same learner-promotion reasoning as for PreVote —
     * see {@link #handlePreVoteReq(Learner, Message.RequestPreVote)}.
     *
     * @param l       the current learner role
     * @param voteReq the incoming vote request
     */
    private void handleVoteReq(Learner l, Message.RequestVote voteReq) throws StorageException {
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
     * <p><b>Follower lease protection (higher-term only):</b> before
     * adopting a higher term, the follower checks whether the leader
     * lease is still active. If all of the following hold, the vote is
     * silently ignored — the follower does not adopt the higher term:</p>
     * <ol>
     *   <li>{@code checkQuorum} is enabled</li>
     *   <li>The follower knows a leader ({@code hasLeader})</li>
     *   <li>The election timer has not reached the base election
     *       timeout — meaning the follower recently heard from a
     *       leader</li>
     *   <li>The election is not a
     *       {@link ElectionCause#LEADER_TRANSFER}</li>
     * </ol>
     *
     * <p><b>Why the lease check must happen before
     * {@code becomeFollower}:</b> {@code becomeFollower} clears the
     * leader reference (the new term has no confirmed leader). If we
     * called it first, {@code hasLeader} would always be false and the
     * lease check would never trigger — completely defeating the lease
     * protection.</p>
     *
     * <p>For same-or-lower-term votes, no term change occurs so the
     * lease check is handled inside {@link #canGrantVote} via the
     * {@code hasLeader} parameter.</p>
     *
     * @param f       the current follower role
     * @param voteReq the incoming vote request
     */
    private void handleVoteReq(Follower f, Message.RequestVote voteReq) throws StorageException {
        if (voteReq.term() > term) {
            // Lease check: recently heard from a leader → reject disruptive vote.
            if (config.leaderLivenessPolicy() == LeaderLivenessPolicy.QUORUM_VERIFIED && f.hasLeader() && !f.isElectionTimedOut() && voteReq.cause() != ElectionCause.LEADER_TRANSFER) {
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
     * Core vote eligibility check for real elections. Determines whether
     * this node can grant its vote to the requesting candidate.
     *
     * <p><b>Invariant:</b> {@code term == voteReq.term()} by the time
     * this method is called. Callers must adopt any higher term (via
     * becomeFollower/becomeLearner) before invoking this, so the decision
     * is always made at the correct term.</p>
     *
     * <p>A vote is granted when <b>both</b> conditions hold:</p>
     * <ol>
     *   <li><b>Eligible to vote for the sender:</b>
     *     <ul>
     *       <li>Haven't voted yet AND (no known leader OR this is a
     *           {@link ElectionCause#LEADER_TRANSFER} — transfer elections
     *           bypass the leader-exists check because the leader itself
     *           authorized the handoff)</li>
     *       <li>OR: already voted for this same sender (idempotent
     *           re-grant for retransmitted vote requests)</li>
     *     </ul>
     *   </li>
     *   <li><b>Sender's log is at least as up-to-date as ours</b>
     *       (Raft §5.4.1 Election restriction — prevents electing a
     *       leader with a stale log)</li>
     * </ol>
     *
     * <p><b>Leader lease interaction:</b> the {@code hasLeader} parameter
     * acts as a same-term lease check. For higher-term votes, the lease
     * check is done <em>before</em> this method by the caller (see the
     * Follower and Leader vote handlers). By the time we get here after
     * a higher-term transition, {@code hasLeader} is false
     * (becomeFollower cleared it), so the check here only matters for
     * same-term votes where no term transition occurred.</p>
     *
     * @param from         the candidate requesting the vote
     * @param lastLogTerm  the candidate's last log entry term
     * @param lastLogIndex the candidate's last log entry index
     * @param hasLeader    whether this node currently knows of a leader
     * @param cause        why the election was initiated (controls lease bypass)
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
     * <p>If the response carries a higher term, step down immediately —
     * someone else has moved on and this election is stale. Otherwise,
     * tally the vote and act on the result:</p>
     * <ul>
     *   <li>WON: become leader and append a placeholder entry to commit
     *       entries from prior terms (Raft leader completeness
     *       requirement).</li>
     *   <li>LOST: majority rejected — fall back to follower at the
     *       current term.</li>
     *   <li>PENDING: still waiting for more votes, do nothing.</li>
     * </ul>
     *
     * @param c   the current Candidate role holding the vote tally
     * @param res the incoming vote response
     */
    private void handleVoteResponse(Candidate c, Message.RequestVoteResponse res) throws StorageException {
        // Higher term in response: another node has advanced.
        // Step down — this election is obsolete.
        if (res.term() > term) {
            becomeFollower(res.term());
            return;
        }
        c.recordVote(res.from(), res.voteGranted());
        var result = membership.voteResult(c.voteQuery());
        switch (result) {
            // Append a no-op entry so the leader can commit entries from prior terms.
            case VoteResult.WON -> appendPlaceholderEntry(becomeLeader());
            // Majority rejected — step down at current term (not response's term,
            // since we already checked higher term above).
            case VoteResult.LOST -> becomeFollower(term);
            case VoteResult.PENDING -> {
            }
        }
    }

    // ────────────────────── PROPOSAL HANDLING ──────────────────────

    /**
     * Handles a data proposal on the leader — the only role that can
     * actually append entries to the replicated log.
     *
     * <p>Proposals are rejected (with a notification) in these cases:</p>
     * <ol>
     *   <li>Empty data — nothing to propose</li>
     *   <li>Leader removed from cluster — a config change removed us, but
     *       we haven't stepped down yet. Accepting proposals in this
     *       window would append entries that can never reach quorum.</li>
     *   <li>Leadership transfer in progress — we're handing off to another
     *       node. Accepting proposals would extend the log and delay the
     *       transferee from catching up.</li>
     *   <li>Uncommitted size limit exceeded — back-pressure. Too many
     *       entries are in-flight (appended but not yet committed).
     *       Accepting more would risk unbounded memory growth.</li>
     * </ol>
     *
     * <p>On success: entries are stamped with term/index, appended to the
     * unstable log, a self-ack is queued (via messagesAfterAppend), and
     * AppendEntries messages are broadcast to all peers.</p>
     *
     * @param l the current leader role
     * @param p the incoming data proposal
     */
    private void handleProposal(Leader l, Message.DataProposal p) throws StorageException {
        if (p.data().isEmpty()) {
            reject(DataDropReason.NO_DATA);
            return;
        }
        // Config change may have removed this leader — check before appending.
        if (!membership.isMember(id)) {
            reject(DataDropReason.REMOVED_FROM_CLUSTER);
            return;
        }
        // During transfer, the leader freezes to let the transferee catch up.
        if (l.isLeaderTransferInProgress()) {
            reject(DataDropReason.LEADER_TRANSFER_IN_PROGRESS);
            return;
        }
        if (!appendProposalEntries(l, p.data())) {
            reject(DataDropReason.EXCEEDS_UNCOMMITTED_SIZE);
            return;
        }
        broadcastAppend(l);

    }

    private boolean canForwardDataProposal(boolean hasLeader) {
        if (!hasLeader) {
            reject(DataDropReason.NO_LEADER);
            return false;
        }

        if (config.proposalHandleMode() == ProposalHandleMode.DROP) {
            reject(DataDropReason.FORWARDING_DISABLED);
            return false;
        }

        return true;
    }


    private boolean canForwardMembershipChangeProposal(boolean hasLeader) {
        if (!hasLeader) {
            reject(MembershipChangeDropReason.NO_LEADER);
            return false;
        }

        if (config.proposalHandleMode() == ProposalHandleMode.DROP) {
            reject(MembershipChangeDropReason.FORWARDING_DISABLED);
            return false;
        }

        return true;
    }
    /**
     * Handles a data proposal on a follower. Followers can't append to
     * the log, so they either forward the proposal to the leader or
     * drop it.
     *
     * <p>Forwarding puts the proposal into the messages buffer addressed
     * to the leader. The node layer is responsible for actually sending
     * it over the network. The leader will then process it as if it were
     * a local proposal.</p>
     *
     * <p>Note: forwarded proposals can still be dropped by the leader
     * (e.g., if the leader is transferring, or uncommitted size is
     * exceeded). There is no acknowledgment back to the follower — the
     * client must handle retries.</p>
     *
     * @param f the current follower role
     * @param p the incoming data proposal
     */
    private void handleProposal(Follower f, Message.DataProposal p) {
        if (!canForwardDataProposal(f.hasLeader())) {
            return;
        }

        send(new Message.DataProposal(f.leaderId(), id, p.data()));
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
        if (!canForwardDataProposal(l.hasLeader())) {
            return;
        }

        send(new Message.DataProposal(l.leaderId(), id, p.data()));
    }

    // ────────────────────── LOG REPLICATION ──────────────────────

    /**
     * Core append path for the leader. Checks uncommitted size budget,
     * appends entries to the unstable log, and sends a self-ack via
     * messagesAfterAppend.
     *
     * <p>The self-ack is an AppendEntriesResponse addressed to ourselves.
     * It flows through messagesAfterAppend so it is only processed AFTER
     * the entries are durably persisted. When handled, it advances the
     * leader's own match index, which can then advance the commit index
     * if quorum is reached.</p>
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
        for (var target : l.peers()) {
            trySendAppend(l, target);
        }
    }

    private void broadcastProbe(Leader l) throws StorageException {
        for (var target : l.peers()) {
            trySendAppend(l, target, false);
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
     * Attempts to send an AppendEntries to a single peer based on its
     * progress.
     *
     * <p>The message includes:</p>
     * <ul>
     *   <li>prevLogIndex / prevLogTerm: the entry just before the ones
     *       being sent, so the follower can verify log consistency</li>
     *   <li>entries: the new entries to replicate (may be empty)</li>
     *   <li>leaderCommit: so the follower can advance its commit
     *       index</li>
     * </ul>
     *
     * <p>When the peer's inflight buffer is full (canReplicateMessages
     * returns false), an empty AppendEntries is sent instead. This acts
     * as a probe — if all inflight messages were dropped by the network,
     * replication would stall because the peer never responds and
     * inflights never clear. The empty message eventually reaches the
     * peer, prompting a response that clears the backlog.</p>
     *
     * <p>If the prevLogIndex has been compacted (log truncated), we can't
     * build an AppendEntries — fall back to sending a snapshot
     * instead.</p>
     *
     * @param l                   the current leader role
     * @param target              the peer to send to
     * @param canSendEmptyEntries if false, skip sending when there are
     *                            no new entries (used by the drain loop
     *                            to avoid pointless empty messages)
     * @return true if a message was sent, false if paused or snapshot failed
     */
    private boolean trySendAppend(Leader l, NodeId target, boolean canSendEmptyEntries) throws StorageException {
        var progress = l.progress(target);
        // Flow control: don't send if this peer is paused.
        // Probe: waiting for previous response. Replicate: inflights full.
        // Snapshot: waiting for snapshot to complete.
        if (progress.isPaused()) return false;

        try {
            // prevLogIndex and prevLogTerm: the follower verifies it has this
            // entry before accepting the new ones (log consistency check).
            var lastIndex = progress.next() - 1;
            var lastTerm = log.term(lastIndex);

            // If cannot replicate messages i.e. Inflight full — send empty AppendEntries as
            // a heartbeat-like probe.
            // The follower will respond, clearing inflights and unblocking the pipeline.
            var nextEntries = progress.canReplicateMessages() ? log.entries(progress.next(), config.maxMsgSize()) : List.<Entry>of();

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
    private boolean appendProposalEntries(Leader l, List<? extends Payload> data) throws StorageException {
        var nextIndex = log.lastIndex() + 1;
        var dataEntries = new ArrayList<Entry>(data.size());
        for (var entryData : data) {
            dataEntries.add(new Entry.Data(term, nextIndex++, entryData));
        }
        return appendEntries(l, dataEntries);
    }

    /**
     * Appends a single empty entry when a new leader is elected.
     *
     * <p>This is required by Raft §5.4.2 — a leader can only commit
     * entries from its own term. The placeholder ensures there's at least
     * one entry in the current term, allowing prior-term entries to be
     * committed indirectly.</p>
     *
     * @param l the newly elected leader
     * @return true if appended (should always succeed for an empty entry)
     */
    private boolean appendPlaceholderEntry(Leader l) throws StorageException {
        return appendEntries(l, List.of(new Entry.Placeholder(term, log.lastIndex() + 1)));
    }

    // ────────────────────── HANDLING APPEND ENTRIES RESPONSE ──────────────────────

    /**
     * Handles an AppendEntriesResponse on the leader. This is the main
     * replication feedback loop — the leader learns what each peer has
     * accepted or rejected, and adjusts its tracking accordingly.
     *
     * <p>Also handles the leader's own self-ack (from
     * messagesAfterAppend), which is how the leader's match index
     * advances after its entries are durably persisted.</p>
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
     * Handles a successful AppendEntriesResponse — the peer accepted our
     * entries.
     *
     * <p>This is the core of the replication feedback loop. Each
     * successful response moves the system forward in up to five
     * ways:</p>
     * <ol>
     *   <li><b>Advance the peer's match index</b> — we now know entries
     *       up to {@code aer.index()} are durably stored on that
     *       peer.</li>
     *   <li><b>Promote the peer's replication state</b> — once the match
     *       point is found (Probe → Replicate), we can pipeline entries
     *       without waiting for each ack, dramatically increasing
     *       throughput.</li>
     *   <li><b>Advance the global commit index</b> — if this response
     *       pushes the peer past the quorum threshold, new entries
     *       become committed. Once committed, we broadcast immediately
     *       so all nodes can apply entries to their state machines
     *       without waiting for the next heartbeat.</li>
     *   <li><b>Drain queued entries</b> — after freeing inflight slots
     *       (acked entries are removed from the inflight tracker) or
     *       transitioning to Replicate (which resets inflights), there
     *       may be room to send more batches to this peer.</li>
     *   <li><b>Complete leadership transfer</b> — if this peer is the
     *       transfer target and is now fully caught up, send TimeoutNow
     *       to trigger the target's immediate election.</li>
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
        // The match point is established. Switch to the fast path where we
        // pipeline multiple batches without waiting for individual acks.
        // Replicate uses an inflight window for backpressure instead of
        // the one-at-a-time Probe throttle.
        //
        // Snapshot → Probe → Replicate:
        // The peer applied the snapshot and is now responding to AppendEntries.
        // Before switching to Replicate, we need match + 1 >= log.firstIndex():
        // this means we can actually send the next entry after the peer's match
        // point without hitting a compacted region. The two-step transition
        // (Probe first, then Replicate) lets becomeProbe properly set next
        // based on the snapshot's match, then becomeReplicate starts the
        // inflight tracking from that point.
        //
        // Replicate/ReplicatePaused:
        // Already in fast mode. tryUpdate freed the acked inflight entries
        // (via removeMessagesUpTo), making room for new batches. No state
        // change needed.
        switch (progress.state()) {
            case ReplicationState.ProbePaused _, ReplicationState.Probe _ -> progress.becomeReplicate();
            case ReplicationState.Snapshot _ when progress.match() + 1 >= log.firstIndex() -> {
                progress.becomeProbe();
                progress.becomeReplicate();
            }
            default -> {
            }
        }

        // --- Step 3: Advance global commit ---
        //
        // Raft's commit rule: an entry is committed when it is stored on a
        // majority of voters AND it was created in the current term. The term
        // check prevents a subtle safety issue where a leader from a previous
        // term could incorrectly commit entries from an earlier term (see
        // Raft paper §5.4.2, Figure 8).
        //
        // majorityAgreed returns the highest value where a majority of
        // voters have match >= that value (the median of all match values
        // in a sorted array). Here, it gives the highest log index
        // replicated to a majority.
        var isMessageFromAnotherNode = !aer.from().equals(id);
        var indexReplicatedAcrossMajority = membership.majorityAgreed(l.matchIndexer());

        if (log.tryCommit(term, indexReplicatedAcrossMajority)) {
            // Commit advanced — two things to do before broadcasting:
            //
            // 1. Release deferred reads: if this is the leader's first commit
            // in the current term, committedInCurrentTerm() just became true.
            // Any read requests that arrived before this point were deferred
            // (leader couldn't serve them without proving its commit index is
            // authoritative). Now they can proceed through the normal path.
            releaseDeferredReadIndex(l);
            // 2. Broadcast to all peers so they learn the new commit index
            // immediately. Without this, peers would only learn about new
            // commits via the next heartbeat, adding up to one heartbeat
            // interval of unnecessary latency.
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
            while (trySendAppend(l, aer.from(), false)) {
            }
            ;
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
     * Handles a rejected AppendEntriesResponse — the peer's log didn't
     * match at prevLogIndex, so we need to backtrack and find the correct
     * match point.
     *
     * <h3>The problem: divergent logs after partitions</h3>
     *
     * <p>Under normal conditions (no partitions), the follower's log is a
     * prefix of the leader's. The first rejection reveals where the
     * follower's log ends, and the next probe succeeds — one round
     * trip.</p>
     *
     * <p>But after network partitions or overloaded systems, logs can
     * diverge significantly. Naively probing entry by entry in decreasing
     * order costs:</p>
     *
     * <pre>
     *   round trips = (length of diverging tail) × (network RTT)
     * </pre>
     *
     * <p>which can take hours and cause outages for long divergent
     * tails.</p>
     *
     * <p>To fix this, both the follower and the leader cooperate to skip
     * entire term ranges. The result: at most <b>O(distinct terms)</b>
     * round trips instead of O(divergent entries). The leader-side
     * optimization is applied in this method. For the follower-side
     * counterpart, see
     * {@link #handleAppendEntriesForSameOrHigherTerm}.</p>
     *
     * <h3>Leader-side optimization (applied here)</h3>
     *
     * <p>The follower's rejection includes {@code indexHint} and
     * {@code termHint} — its best guess for where the logs might match
     * and the term at that position in its log.</p>
     *
     * <p>The leader searches its OWN log backward from
     * {@code indexHint} for the first index where the leader's
     * term ≤ {@code termHint}.</p>
     *
     * <p><b>Why this works:</b> the follower said "my term at index N
     * is T." Log terms only increase within a log, so every index ≤ N
     * on the follower has term ≤ T. Any leader log entry with a term
     * <i>higher</i> than T at an index ≤ N can never match the follower
     * at that position — the follower's term there is at most T. Skip
     * all of them.</p>
     *
     * <p><b>Example</b> (follower is shorter with lower terms — leader
     * has the higher divergent tail):</p>
     *
     * <pre>
     *   idx:      1  2  3  4  5  6  7  8  9  10  11  12
     *   Leader:   1  3  3  3  5  5  5  7  7   7   7   7
     *   Follower: 1  1  1  2  2  2  2
     *   (common prefix: idx 1 only; divergence: idx 2-12)
     * </pre>
     *
     * <ol>
     *   <li>Leader probes at (idx=12, prevLogTerm=7). Follower doesn't
     *       have idx 12. Follower rejects with indexHint=7, termHint=2
     *       (its last index and the term there).</li>
     *
     *   <li><b>Naive approach</b> — probe one by one from idx 7
     *       downward:
     *
     * <pre>
     *   idx 7: leader term=5, follower term=2 → mismatch  (round trip 2)
     *   idx 6: leader term=5, follower term=2 → mismatch  (round trip 3)
     *   idx 5: leader term=5, follower term=2 → mismatch  (round trip 4)
     *   idx 4: leader term=3, follower term=2 → mismatch  (round trip 5)
     *   idx 3: leader term=3, follower term=1 → mismatch  (round trip 6)
     *   idx 2: leader term=3, follower term=1 → mismatch  (round trip 7)
     *   idx 1: leader term=1, follower term=1 → match!    (round trip 8)
     * </pre>
     *
     *       <b>8 round trips total.</b></li>
     *
     *   <li><b>With leader-side optimization</b> — search leader's log
     *       for term ≤ 2 starting from indexHint=7:
     *
     * <pre>
     *   findConflictEntryByTerm(termHint=2, indexHint=7):
     *     idx 7: leader term=5 &gt; 2, skip    (term 5 covers idx 5-7)
     *     idx 4: leader term=3 &gt; 2, skip    (term 3 covers idx 2-4)
     *     idx 1: leader term=1 ≤ 2, found!
     * </pre>
     *
     *       Leader jumps straight to probing idx 1 — match!
     *       <b>2 round trips total</b> (initial probe + final probe).
     *       Walked through <b>3 distinct leader terms</b> (5, 3, 1)
     *       instead of 7 individual entries.</li>
     * </ol>
     *
     * <p><b>Complexity:</b> O(distinct terms in the leader's divergent
     * region between indexHint and the match point). In this example:
     * 3 term boundaries instead of 7 entries — saving 6 round trips.</p>
     *
     * <p><b>When this optimization alone isn't enough:</b> if the
     * follower's divergent tail has <i>higher</i> terms than the
     * leader's, the leader's {@code findConflictEntryByTerm} immediately
     * finds leader term ≤ termHint at the current index (since the
     * leader's terms are already low) — no skipping occurs. It
     * degenerates to near-linear probing. The follower-side optimization
     * in {@link #handleAppendEntriesForSameOrHigherTerm} handles this
     * case by searching the follower's own log to skip its high-term
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
        var nextProbeIndex = aer.termHint() > 0 ? log.findConflictEntryByTerm(aer.termHint(), aer.indexHint()).index() : aer.indexHint();

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

    // ────────────────────── APPEND ENTRIES — RECEIVING (Learner/Voter) ──────────────────────

    /**
     * Handles an AppendEntries received while this node is a Learner.
     *
     * <p>Same logic as the voter handler, except we stay as a Learner
     * (via becomeLearner) instead of transitioning to Follower.</p>
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
     * Handles an AppendEntries received by a Voter role (Follower,
     * Candidate, PreCandidate, or Leader).
     *
     * <ul>
     *   <li><b>Follower:</b> confirms the leader is alive, resets
     *       election timer, and processes the entries. becomeFollower
     *       reuses the existing Follower instance when the term hasn't
     *       changed.</li>
     *   <li><b>Candidate/PreCandidate:</b> receiving AppendEntries at
     *       the same term means another node already won the election.
     *       Step down to Follower (becomeFollower handles term adoption
     *       and leader tracking).</li>
     *   <li><b>Leader:</b> receiving AppendEntries at a higher term
     *       means a new leader was elected. Step down to Follower.</li>
     * </ul>
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
     * Core AppendEntries processing after term checks and role
     * transitions. At this point, our term matches the leader's term.
     *
     * <h3>Three outcomes</h3>
     *
     * <ol>
     *   <li><b>Stale message:</b> prevLogIndex is behind our committed
     *       index. We already have those entries committed. Respond with
     *       success and our committed index so the leader can update its
     *       tracking.</li>
     *   <li><b>Log consistency check passes:</b> we have an entry at
     *       prevLogIndex with the matching prevLogTerm. Append entries,
     *       advance commit index, and respond with success.</li>
     *   <li><b>Log consistency check fails:</b> our log diverges at
     *       prevLogIndex. Respond with a rejection and provide optimized
     *       hints so the leader can backtrack efficiently. See the
     *       follower-side optimization below.</li>
     * </ol>
     *
     * <h3>Follower-side optimization (applied in outcome 3)</h3>
     *
     * <h4>The problem: large divergent follower logs</h4>
     *
     * <p>When logs diverge after partitions, the leader-side optimization
     * (see {@link #handleFailedAppend(Leader, Message.AppendEntriesResponse, PeerProgress)})
     * works well when the <i>leader's</i> divergent tail has higher terms
     * than the follower's — it skips entire term ranges in the leader's
     * log. But it breaks down when the situation is reversed: the
     * <i>follower's</i> divergent tail has higher terms than the
     * leader's.</p>
     *
     * <p>This can happen after crash/recovery sequences: a follower was
     * briefly elected leader at a higher term, appended entries, then
     * crashed before committing — those uncommitted entries remain as a
     * high-term divergent tail. In this scenario, the leader-side
     * optimization provides no benefit because the leader's terms in the
     * divergent region are already low (≤ every follower termHint), so
     * its backward search immediately lands at the current index every
     * time — zero skipping, one index per round trip,
     * O(divergent entries).</p>
     *
     * <p><b>Example</b> (follower has a high-term divergent tail):</p>
     *
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
     *       Follower: term at 12 is 7 ≠ 8. Rejects with indexHint=12,
     *       termHint=7. <br/>
     *       Leader-side: findConflictEntryByTerm(termHint=7, indexHint=12):
     *
     * <pre>
     *     idx 12: leader term=8 &gt; 7, skip
     *     idx 11: leader term=3 ≤ 7, found!
     * </pre>
     *
     *       <p>Leader skipped 1 entry. Probes at idx 11 next.</li>
     *
     *   <li><b>Round 2:</b> Leader probes at (idx=11, prevLogTerm=3).
     *       Follower: term at 11 is 7 ≠ 3. Rejects with indexHint=11,
     *       termHint=7. <br/>
     *       Leader-side: findConflictEntryByTerm(termHint=7, indexHint=11):
     *
     * <pre>
     *     idx 11: leader term=3 ≤ 7, found immediately!
     * </pre>
     *
     *       <b>No skipping</b> — leader's term (3) is already ≤
     *       termHint (7). Leader moves to idx 10.</li>
     *
     *   <li><b>Round 3:</b> Leader probes at (idx=10, prevLogTerm=3).
     *       Follower: term at 10 is 7 ≠ 3. Rejects with indexHint=10,
     *       termHint=7. <br/>
     *       Leader-side: findConflictEntryByTerm(termHint=7, indexHint=10):
     *
     * <pre>
     *     idx 10: leader term=3 ≤ 7, found immediately!
     * </pre>
     *
     *       <b>No skipping again.</b> Leader moves to idx 9.</li>
     *
     *   <li><b>Rounds 4-9:</b> Same pattern for idx 9, 8, 7, 6, 5, 4.
     *       At each index, leader term is 3, follower's termHint is
     *       always ≥ 4 (follower terms are 4, 5, 6, 7). The leader-side
     *       search returns the current index immediately every time —
     *       zero skipping.
     *
     * <pre>
     *   Round 4: idx=9,  leader term=3 ≤ 6 → found immediately
     *   Round 5: idx=8,  leader term=3 ≤ 6 → found immediately
     *   Round 6: idx=7,  leader term=3 ≤ 5 → found immediately
     *   Round 7: idx=6,  leader term=3 ≤ 5 → found immediately
     *   Round 8: idx=5,  leader term=3 ≤ 4 → found immediately
     *   Round 9: idx=4,  leader term=3 ≤ 4 → found immediately
     * </pre>
     *
     *       </li>
     *
     *   <li><b>Round 10:</b> Leader probes at idx 3: leader term=2,
     *       follower term=2 → match!</li>
     * </ol>
     *
     * <p><b>~10 round trips</b> — the leader-side optimization provided
     * zero benefit from Round 2 onward because the leader's terms (all 3)
     * are consistently ≤ the follower's termHints (4, 5, 6, 7). The
     * search always lands at the current index. This is still
     * O(divergent entries) — 9 divergent entries, ~10 round trips.</p>
     *
     * <h4>The solution: follower-side term-range skipping</h4>
     *
     * <p>The follower can do better than naively returning its last index
     * and term. The key insight: the leader's {@code prevLogTerm} reveals
     * information about the leader's entire log prefix — since terms
     * only increase within a log, every entry before prevLogIndex in the
     * leader's log has term ≤ prevLogTerm.</p>
     *
     * <p>So the follower searches backward through its OWN log for the
     * largest index where its term ≤ prevLogTerm. Any follower entry
     * with a term <i>higher</i> than prevLogTerm can never match the
     * leader at that position (the leader's term there is at most
     * prevLogTerm). Skip them all.</p>
     *
     * <p><b>Same example, now with follower-side optimization:</b></p>
     *
     * <pre>
     *   idx:      1  2  3  4  5  6  7  8  9  10  11  12
     *   Leader:   1  2  2  3  3  3  3  3  3   3   3   8
     *   Follower: 1  2  2  4  4  5  5  6  6   7   7   7
     * </pre>
     *
     * <ol>
     *   <li><b>Round 1:</b> Leader probes at (idx=12, prevLogTerm=8).
     *       Follower: term at 12 is 7 ≠ 8. Follower-side:
     *
     * <pre>
     *   findConflictEntryByTerm(prevLogTerm=8, idx=12):
     *     idx 12: our term=7 ≤ 8, found immediately!
     * </pre>
     *
     *       <p>No skipping (prevLogTerm too high). Return indexHint=12,
     *       termHint=7. <br/>
     *       Leader-side: skips idx 12 (term 8 &gt; 7), lands on idx 11
     *       (term 3 ≤ 7). Probes at idx 11 next.</li>
     *
     *   <li><b>Round 2:</b> Leader probes at (idx=11, prevLogTerm=3).
     *       Follower: term at 11 is 7 ≠ 3. Follower-side:
     *
     * <pre>
     *   findConflictEntryByTerm(prevLogTerm=3, idx=11):
     *     idx 11: our term=7 &gt; 3, skip   (term 7 covers idx 10-11)
     *     idx  9: our term=6 &gt; 3, skip   (term 6 covers idx 8-9)
     *     idx  7: our term=5 &gt; 3, skip   (term 5 covers idx 6-7)
     *     idx  5: our term=4 &gt; 3, skip   (term 4 covers idx 4-5)
     *     idx  3: our term=2 ≤ 3, found!
     * </pre>
     *
     *       <p>Return indexHint=3, termHint=2. Jumped from idx 11 to
     *       idx 3 — skipped 8 entries in one round trip.</li>
     *
     *   <li><b>Round 3:</b> Leader probes at idx 3: leader term=2,
     *       our term=2 → <b>match!</b></li>
     * </ol>
     *
     * <p><b>3 round trips total</b> vs ~10 with leader-side only.</p>
     *
     * <p><b>Complexity:</b> O(distinct terms in the follower's divergent
     * region). In this example: walked through 4 distinct follower terms
     * (7, 6, 5, 4) to skip from idx 11 to idx 3 in one round trip.
     * Together with the leader-side optimization (which skips ranges
     * where the <i>leader's</i> terms are too high), both sides
     * cooperate to converge in at most O(distinct terms) round trips
     * across the divergent region of either log.</p>
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
        // "Does this follower have an entry at prevLogIndex with prevLogTerm?"
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

    // ────────────────────── QUORUM CHECK ──────────────────────

    /**
     * Verifies that the leader still has the support of a majority of
     * the cluster.
     *
     * <p>This is invoked on every election timeout interval (not
     * heartbeat interval) to detect network partitions where the leader
     * can no longer reach a quorum. Without this check, a partitioned
     * leader would continue to accept proposals indefinitely even though
     * those entries can never be committed.</p>
     *
     * <p><b>How it works:</b></p>
     *
     * <ol>
     *   <li>Collects the {@code active} flag from each peer's
     *       {@link PeerProgress}. A peer is marked active whenever the
     *       leader receives any message from it (heartbeat response,
     *       append response, etc.). The leader itself is always counted
     *       as active.</li>
     *   <li>After collecting, <em>all peer flags are reset to
     *       inactive</em> (the leader's own flag stays active). This
     *       means each peer must respond again before the next quorum
     *       check or it will be counted as inactive.</li>
     *   <li>The collected votes are passed to
     *       {@link MembershipConfig#voteResult(Function)} which
     *       determines if a majority of voters are active.</li>
     *   <li>If the result is anything other than {@code WON} (including
     *       {@code PENDING}, which means not enough active peers to form
     *       a definitive majority), the leader steps down to
     *       Follower.</li>
     * </ol>
     *
     * <p><b>Example — 5-node cluster {A, B, C, D, E}, A is
     * leader:</b></p>
     *
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
        if (membership.voteResult(id -> Optional.ofNullable(votes.get(id))) != VoteResult.WON) {
            becomeFollower(term);
        }
    }

    // ────────────────────── HANDLING HEARTBEAT ──────────────────────

    /**
     * Broadcasts a heartbeat to every peer in the cluster (excluding
     * the leader itself).
     *
     * <p>Heartbeats serve four purposes:</p>
     *
     * <ol>
     *   <li><b>Leader authority:</b> Resets each follower's election
     *       timer, preventing unnecessary elections while the leader is
     *       alive.</li>
     *   <li><b>Commit advancement:</b> Carries the leader's commit
     *       index (capped to each follower's match) so followers can
     *       apply committed entries even when there are no new log
     *       entries to replicate.</li>
     *   <li><b>Liveness signal:</b> Heartbeat responses mark peers as
     *       active for quorum checks (see
     *       {@link #checkQuorum(Leader)}).</li>
     *   <li><b>Read index confirmation:</b> Each heartbeat carries a
     *       monotonically increasing sequence number. Followers echo it
     *       in their response. When a majority has acked a given seq,
     *       all pending reads registered before that seq are confirmed
     *       (see {@link ReadIndex}).</li>
     * </ol>
     *
     * <p>The seq is obtained via {@link Leader#nextHeartbeatSeq()} — a
     * single increment per broadcast ensures all peers in this round
     * carry the same seq, and multiple reads batched between broadcasts
     * share the same confirmation round.</p>
     *
     * @see #sendHeartbeat(Leader, NodeId, long) for the per-peer logic
     */
    private void broadcastHeartbeat(Leader l) {
        var seq = l.nextHeartbeatSeq();
        for (var peer : l.peers()) {
            sendHeartbeat(l, peer, seq);
        }
    }

    /**
     * Sends a heartbeat to a single peer.
     *
     * <p>The commit index carried in the heartbeat is
     * {@code min(progress.match(), log.committed())}. This cap is
     * critical: the leader must not tell a follower to commit beyond
     * what it has actually replicated. If the follower's log is behind,
     * advancing its commit index past its log would cause it to apply
     * entries it does not have.</p>
     *
     * <p>After sending, we record the committed index we communicated
     * via {@link PeerProgress#sentCommit(long)} so that subsequent
     * AppendEntries can avoid redundantly re-sending the same commit
     * index.</p>
     *
     * @param l      the current leader role state
     * @param target the peer to send the heartbeat to
     * @param seq    the heartbeat sequence number — echoed by the
     *               follower in its response for read index correlation
     */
    private void sendHeartbeat(Leader l, NodeId target, long seq) {
        var progress = l.progress(target);
        // Peer may have been removed by a membership change.
        if (progress == null) {
            return;
        }

        var commitIndex = Math.min(progress.match(), log.committed());
        send(new Message.Heartbeat(target, id, term, commitIndex, seq));
        progress.sentCommit(commitIndex);
    }

    /**
     * Handles a heartbeat received by a <b>voter</b> (Follower,
     * Candidate, or PreCandidate).
     *
     * <p>Follows the same term-handling pattern as
     * {@link #handleAppendEntriesForVoter(Message.AppendEntries)}:</p>
     *
     * <ul>
     *   <li><b>Lower term:</b> Stale heartbeat from a deposed leader.
     *       Respond so the sender discovers the higher term and steps
     *       down. No state change.</li>
     *   <li><b>Same or higher term:</b> Legitimate heartbeat from the
     *       current (or new) leader. Transition to Follower (resets
     *       election timer, confirms leader), advance local commit
     *       index, and respond.</li>
     * </ul>
     *
     * <p>For Candidates and PreCandidates receiving a same-or-higher
     * term heartbeat, {@code becomeFollower} causes them to abandon
     * their election — a leader already exists for this term.</p>
     */
    private void handleHeartbeatForVoter(Message.Heartbeat hb) throws StorageException {
        // Lower term: stale heartbeat from a deposed leader.
        // Respond so the sender discovers the higher term and steps down.
        if (hb.term() < term) {
            send(new Message.HeartbeatResponse(hb.from(), id, term, hb.sequence()));
            return;
        }

        becomeFollower(hb.term(), hb.from());
        log.commitTo(hb.leaderCommit());
        send(new Message.HeartbeatResponse(hb.from(), id, term, hb.sequence()));
    }

    /**
     * Handles a heartbeat received by a <b>Learner</b>.
     *
     * <p>Mirrors {@link #handleHeartbeatForVoter(Message.Heartbeat)}
     * but calls {@link #becomeLearner(long, NodeId)} instead of
     * {@code becomeFollower} to preserve the node's learner
     * (non-voting) status.</p>
     */
    private void handleHeartbeat(Learner l, Message.Heartbeat hb) throws StorageException {
        // Lower term: stale heartbeat from a deposed leader.
        // Respond so the sender discovers the higher term and steps down.
        if (hb.term() < term) {
            send(new Message.HeartbeatResponse(hb.from(), id, term, hb.sequence()));
            return;
        }

        becomeLearner(hb.term(), hb.from());
        log.commitTo(hb.leaderCommit());
        send(new Message.HeartbeatResponse(hb.from(), id, term, hb.sequence()));
    }

    /**
     * Handles a heartbeat response received by the Leader.
     *
     * <p><b>Term handling:</b></p>
     *
     * <ul>
     *   <li><b>Lower term:</b> Stale response from a previous term.
     *       Silently dropped — no useful information can be extracted
     *       from it.</li>
     *   <li><b>Higher term:</b> Another node has been elected leader.
     *       Step down to Follower.</li>
     *   <li><b>Same term:</b> Process the response (below).</li>
     * </ul>
     *
     * <p><b>Same-term processing:</b></p>
     *
     * <ol>
     *   <li><b>Mark active:</b> The peer is alive and reachable — mark
     *       it active for quorum checks (see
     *       {@link #checkQuorum(Leader)}).</li>
     *   <li><b>Resume flow:</b> If the peer was paused (e.g., Probe
     *       waiting for a response, or Replicate with full in-flights),
     *       resume it. A heartbeat response is proof that the network
     *       path is working, so it's safe to resume sending.</li>
     *   <li><b>Trigger replication:</b> If the peer is in Probe state
     *       (may be recovering from an unreachable report) or is behind
     *       the leader's log, send an AppendEntries. This allows the
     *       peer to:
     *       <ul>
     *         <li>Transition from Probe back to Replicate on a
     *             successful response.</li>
     *         <li>Recover from situations where all in-flight messages
     *             were dropped (the heartbeat response proves
     *             connectivity is restored).</li>
     *         <li>Catch up on entries it hasn't received yet.</li>
     *       </ul>
     *       Note: if the peer is in Snapshot state,
     *       {@code trySendAppend} is a no-op since snapshot-state peers
     *       are always paused.</li>
     *   <li><b>Drain confirmed reads:</b> The response echoes the
     *       heartbeat's seq. {@link Leader#drainAfterAck} records this
     *       ack and checks if the majority-agreed seq has advanced past
     *       any pending read's required seq. If so, those reads are
     *       confirmed and responses are sent (locally via
     *       {@link ReadState} or remotely via
     *       {@link consensus.message.Message.ReadIndexResponse}).</li>
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

        // Heartbeat ack may push majority-agreed seq past pending reads' required seq.
        var ackedReads = l.drainAfterAck(hbr.from(), hbr.sequence(), membership);
        for (var pendingRead : ackedReads) {
            respondToReadIndex(pendingRead.from(), pendingRead.index());
        }

    }

    // ────────────────────── SNAPSHOT HANDLING ──────────────────────

    /**
     * Sends a snapshot to a peer that is too far behind for log-based
     * replication.
     *
     * <p>Only attempted if the peer is recently active — there's no
     * point sending a potentially large snapshot to a peer that isn't
     * responding.</p>
     *
     * <p>On success, the peer transitions to Snapshot state, pausing
     * all AppendEntries until the snapshot is applied and the peer
     * responds.</p>
     *
     * @param target   the peer to send to
     * @param progress the peer's replication progress
     * @return true if snapshot was sent, false if peer inactive or
     *         snapshot unavailable
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
     * except we stay as a Learner (via {@code becomeLearner}) instead
     * of transitioning to Follower. Learners are the most common
     * snapshot recipients — they're typically new nodes catching up on
     * the log.</p>
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
        restore(becomeLearner(is.term(), is.from()), is);
    }

    /**
     * Handles an InstallSnapshot received by a voter role (Follower,
     * Candidate, PreCandidate, or Leader).
     *
     * <p>Follows the same pattern as
     * {@link #handleAppendEntriesForVoter(Message.AppendEntries)}:
     * reject lower-term messages, then call {@code becomeFollower} for
     * both same-term and higher-term messages before processing. This
     * ensures the election timer is reset (the snapshot proves the
     * leader is alive) and the leader identity is confirmed.</p>
     *
     * <p>For Candidate/PreCandidate: receiving a snapshot proves a
     * leader exists in this term — the node steps down to Follower and
     * applies the snapshot, just like it would for an
     * AppendEntries.</p>
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
        restore(becomeFollower(is.term(), is.from()), is);
    }

    /**
     * Role-specific snapshot restore logic after term checks and role
     * transitions.
     *
     * <p>Delegates to {@link #tryRestore(Snapshot)} and sends an
     * AppendEntriesResponse back to the leader in both cases:</p>
     *
     * <ul>
     *   <li><b>Restored</b> (tryRestore returned true): the log was
     *       replaced by the snapshot. The snapshot's membership is
     *       adopted via {@link #restoreSnapshotMembership} which
     *       triggers role-specific side effects (e.g. follower demoted
     *       to learner, learner promoted to voter). Respond with
     *       {@code lastIndex()} — the leader uses this to advance
     *       the peer's match index to the snapshot point.</li>
     *   <li><b>Not restored</b> (tryRestore returned false): the
     *       snapshot was stale, redundant, or rejected. Respond with
     *       {@code committed} — tells the leader where this node
     *       currently stands so it can resume normal replication from
     *       that point.</li>
     * </ul>
     *
     * <p>Both responses are marked as success ({@code true}). Even
     * when the snapshot isn't applied, this node is operational and the
     * leader should update its progress tracking accordingly. This is
     * different from AppendEntries rejection (log divergence) where the
     * response carries {@code false} and divergence hints.</p>
     *
     * @param f  the current follower role (used for membership switch)
     * @param is the InstallSnapshot message
     */
    private void restore(Follower f, Message.InstallSnapshot is) throws StorageException {
        if (tryRestore(is.snapshot())) {
            restoreSnapshotMembership(f, is.snapshot());
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.lastIndex(), 0, 0));
        } else {
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.committed(), 0, 0));
        }
    }

    /**
     * @param l  the current learner role (used for membership switch)
     * @param is the InstallSnapshot message
     * @see #restore(Follower, Message.InstallSnapshot)
     */
    private void restore(Learner l, Message.InstallSnapshot is) throws StorageException {
        if (tryRestore(is.snapshot())) {
            restoreSnapshotMembership(l, is.snapshot());
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.lastIndex(), 0, 0));
        } else {
            send(new Message.AppendEntriesResponse(is.from(), id, term, true, log.committed(), 0, 0));
        }
    }

    /**
     * Replaces the current membership with the snapshot's membership
     * and triggers role-specific side effects via
     * {@link #switchMembership}. A follower may be demoted to learner;
     * a learner may be promoted to voter.
     *
     * @param f        the current follower role (used for demotion check)
     * @param snapshot the restored snapshot containing the new membership
     */
    private void restoreSnapshotMembership(Follower f, Snapshot snapshot) {
        membership = snapshot.membership();
        switchMembership(f);
    }

    /**
     * @param l        the current learner role (used for promotion check)
     * @param snapshot the restored snapshot containing the new membership
     * @see #restoreSnapshotMembership(Follower, Snapshot)
     */
    private void restoreSnapshotMembership(Learner l, Snapshot snapshot) {
        membership = snapshot.membership();
        switchMembership(l);
    }

    /**
     * Attempts to restore this node's state from a snapshot.
     *
     * <p>This is the core snapshot-restore logic with three rejection
     * cases and one success case:</p>
     *
     * <ol>
     *   <li><b>Already committed:</b> the snapshot's index is at or
     *       behind our committed index. We already have all those
     *       entries committed and possibly applied — the snapshot is
     *       stale. Return false.</li>
     *   <li><b>Not a member:</b> this node is not in the snapshot's
     *       membership (not a voter, not a learner). This is a
     *       defense-in-depth check — it should never happen in normal
     *       operation, but if it did (e.g., the node was removed from
     *       the cluster), applying the snapshot would leave the node
     *       in an inconsistent state. Return false.</li>
     *   <li><b>Log already matches:</b> we have an entry at the
     *       snapshot's index with the same term. Our log is consistent
     *       with the snapshot up to that point — no need to replace
     *       it. Just fast-forward the commit index to the snapshot's
     *       index. Return false.</li>
     *   <li><b>Full restore:</b> our log diverges from the snapshot
     *       (or we don't have entries at that index). Replace the
     *       the snapshot and adopt its membership configuration.
     *       Return true.</li>
     * </ol>
     *
     * <p>Note: when membership configuration changes are implemented,
     * the full restore path must also rebuild the progress tracker from
     * the snapshot's membership (switchToConfig). Without this, a node
     * that later becomes leader would have a stale progress tracker.</p>
     *
     * @param snapshot the snapshot to restore from
     * @return true if the log was replaced (full restore), false if
     *         the snapshot was stale, rejected, or the commit was
     *         fast-forwarded
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
        return true;
    }

    /**
     * Handles local feedback about the outcome of a snapshot delivery.
     *
     * <p>This is NOT a network message — it's local feedback from the
     * application/transport layer on the same node that sent the
     * snapshot. After the leader sends an
     * {@link Message.InstallSnapshot}, the peer transitions to Snapshot
     * state and all AppendEntries are paused. This method unblocks the
     * peer when the transport reports back.</p>
     *
     * <p>Two outcomes:</p>
     *
     * <ul>
     *   <li><b>Success:</b> the snapshot was delivered. Transition to
     *       Probe starting from {@code snapshotIndex + 1}. The peer
     *       will send an AppendEntriesResponse after it applies the
     *       snapshot — that response will confirm the match point and
     *       trigger a transition to Replicate for fast pipelining.</li>
     *   <li><b>Failure:</b> the delivery failed (network error, peer
     *       rejected it, etc.). Reset to Probe starting from
     *       {@code match + 1} — we go back to the last confirmed
     *       match point since the snapshot was never applied.</li>
     * </ul>
     *
     * <p>In both cases, the peer is paused after transitioning to
     * Probe. On success, we wait for the peer's
     * AppendEntriesResponse before sending new entries. On failure, we
     * wait for the next heartbeat cycle before retrying.</p>
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
     * <p>Only affects peers in Replicate state. During optimistic
     * pipelining, the leader sends multiple AppendEntries without
     * waiting for responses. If the peer becomes unreachable, those
     * pipelined messages were likely lost. Demoting to Probe switches
     * to conservative one-at-a-time sending, which is more appropriate
     * for an unreliable connection.</p>
     *
     * <p>If the peer is already in Probe (already conservative) or
     * Snapshot (waiting for snapshot to complete), this is a no-op —
     * those states are already prepared for communication
     * failures.</p>
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

    // ───────────────────── LEADERSHIP TRANSFER ──────────────────────────────

    /**
     * Handles a leadership transfer request at the Leader.
     *
     * <p>The application sends {@code TransferLeadership} to gracefully
     * move leadership to a specific voter. The leader does not respond
     * to the application — the transfer outcome is observable by
     * watching who becomes the next leader (via a new leader's
     * {@code AppendEntries}).</p>
     *
     * <p><b>Validation sequence:</b></p>
     *
     * <ol>
     *   <li>Self-transfer — transferring to yourself is a no-op</li>
     *   <li>Unknown/learner — only voters in the current config can
     *       become leader</li>
     *   <li>Duplicate — if a transfer to the same target is already in
     *       progress, don't restart the process</li>
     * </ol>
     *
     * <p>Once validated, {@link Leader#transferLeadership(NodeId)} sets
     * the target and resets the quorum check timer, giving the
     * transferee a full election-timeout window to catch up. If a
     * transfer was already in progress to a <em>different</em> target,
     * it is implicitly aborted — the new target overwrites the old
     * one.</p>
     *
     * <p><b>Catch-up or immediate handoff:</b></p>
     *
     * <ul>
     *   <li><b>Caught up</b> ({@code progress.match() == lastIndex}):
     *       send {@code TimeoutNow} immediately — the transferee
     *       starts its election</li>
     *   <li><b>Behind</b>: send {@code AppendEntries} to replicate the
     *       missing entries. When the transferee's
     *       {@code AppendEntriesResponse} confirms catch-up,
     *       {@link #handleSuccessfulAppend} sends the
     *       {@code TimeoutNow}</li>
     * </ul>
     *
     * <p>While a transfer is active, the leader blocks new proposals
     * (see {@code handleProposal}) to prevent the goal post from
     * moving. The transfer is aborted if the quorum check timer fires
     * before completion (transferee is unreachable or too slow).</p>
     *
     * @param l  the current leader role
     * @param tl the transfer request (carries the target
     *           {@code transferee})
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
     * <p>Followers cannot process leadership transfers — only the
     * leader can. If a leader is known, the request is forwarded to it.
     * If no leader is known (e.g., during an election), the request is
     * dropped because there is no one to forward to.</p>
     *
     * @param f  the current follower role
     * @param tl the transfer request to forward
     */
    private void handleLeadershipTransfer(Follower f, Message.TransferLeadership tl) {
        if (!f.hasLeader()) {
            return;
        }
        send(new Message.TransferLeadership(f.leaderId(), id, tl.transferee()));
    }

    /**
     * Handles a {@code TransferLeadership} request at a Learner.
     *
     * <p>Same forwarding logic as for Followers — learners cannot
     * process transfers, so the request is forwarded to the known
     * leader or dropped if no leader is known.</p>
     *
     * @param l  the current learner role
     * @param tl the transfer request to forward
     */
    private void handleLeadershipTransfer(Learner l, Message.TransferLeadership tl) {
        if (!l.hasLeader()) {
            return;
        }
        send(new Message.TransferLeadership(l.leaderId(), id, tl.transferee()));
    }

    // ────────────────────── MEMBERSHIP CHANGE — PROPOSAL HANDLING ──────────────────────

    /**
     * Handles a membership change proposal (add voter, add learner, or
     * remove).
     *
     * <p>This is the entry point for the <b>first phase</b> of a
     * membership change. Depending on the transition type and symmetric
     * difference of the voter sets, the {@link MembershipChanger} will
     * later decide whether the change is simple (single-phase) or
     * joint (two-phase).</p>
     *
     * <p>Guards enforced before the entry is appended:</p>
     *
     * <ol>
     *   <li>Common gates via {@link #canProceedMembershipChange} —
     *       leader must have self-progress, no leadership transfer in
     *       progress, and no pending (unapplied) config change.</li>
     *   <li>Must <b>not</b> be in joint consensus. Proposing a new
     *       membership change while already in joint is invalid — the
     *       cluster must first complete the current transition via
     *       leave-joint.</li>
     * </ol>
     *
     * <p>If all gates pass, a {@link Entry.MembershipChange} entry is
     * appended to the log and broadcast to followers.</p>
     */
    private void handleMembershipChange(Leader l, Message.MembershipChangeProposal mcp) throws StorageException {
        if (!canProceedMembershipChange(l)) {
            return;
        }

        if (membership.isJoint()) {
            reject(MembershipChangeDropReason.ENTER_WHILE_IN_JOINT_CONSENSUS);
            return;
        }
        var membershipChangeIndex = log.lastIndex() + 1;
        appendMembershipEntry(l, new Entry.MembershipChange(term, membershipChangeIndex, mcp.membershipChanges()), membershipChangeIndex);
    }

    /**
     * Handles a leave-joint proposal — the <b>second phase</b> of a
     * two-phase membership change.
     *
     * <p>Leave-joint drops the old (outgoing) voter set and promotes
     * the incoming set as the sole config. After this, the config is
     * no longer joint and a new membership change can be proposed.</p>
     *
     * <p>Guards enforced:</p>
     *
     * <ol>
     *   <li>Common gates via
     *       {@link #canProceedMembershipChange}.</li>
     *   <li>Must <b>be</b> in joint consensus. Leaving joint when the
     *       config is already non-joint is nonsensical.</li>
     * </ol>
     *
     * <p>A {@link Entry.LeaveJoint} entry is appended to the log and
     * broadcast to followers. This can be triggered explicitly by the
     * application or automatically via
     * {@link #appliedTo(Leader, long, long)} when the transition type
     * is {@code JOINT_AUTO}.</p>
     */
    private void handleLeaveJoint(Leader l) throws StorageException {
        if (!canProceedMembershipChange(l)) {
            return;
        }

        if (!membership.isJoint()) {
            reject(MembershipChangeDropReason.LEAVE_WHILE_NOT_IN_JOINT_CONSENSUS);
            return;
        }

        var membershipChangeIndex = log.lastIndex() + 1;
        appendMembershipEntry(l, new Entry.LeaveJoint(term, membershipChangeIndex), membershipChangeIndex);
    }

    private void handleMembershipChange(Follower f, Message.MembershipChangeProposal mcp) throws StorageException {
        if (!canForwardMembershipChangeProposal(f.hasLeader())) {
            return;
        }

        step(new Message.MembershipChangeProposal(f.leaderId(), id, mcp.membershipChanges()));
    }

    private void handleMembershipChange(Learner l, Message.MembershipChangeProposal mcp) throws StorageException {
        if (!canForwardMembershipChangeProposal(l.hasLeader())) {
            return;
        }

        step(new Message.MembershipChangeProposal(l.leaderId(), id, mcp.membershipChanges()));
    }

    private void handleLeaveJoint(Follower f) throws StorageException {
        if (!canForwardMembershipChangeProposal(f.hasLeader())) {
            return;
        }

        step(new Message.LeaveJointProposal(f.leaderId(), id));
    }

    private void handleLeaveJoint(Learner l) throws StorageException {
        if (!canForwardMembershipChangeProposal(l.hasLeader())) {
            return;
        }

        step(new Message.LeaveJointProposal(l.leaderId(), id));
    }

    /**
     * Common gate checks for any membership change proposal (enter or
     * leave).
     *
     * <p>Three conditions must hold:</p>
     *
     * <ul>
     *   <li>The leader must have progress for itself — a sanity check
     *       that it is properly initialized.</li>
     *   <li>No leadership transfer is in progress — config changes
     *       during a transfer could leave the transferee with a stale
     *       config.</li>
     *   <li>No pending (unapplied) config change in the log — Raft
     *       allows at most one config change in flight. The applied
     *       index must have caught up to
     *       {@code membershipChangeIndex}.</li>
     * </ul>
     *
     * @return true if all gates pass and the proposal can proceed
     */
    private boolean canProceedMembershipChange(Leader l) {
        var progress = l.progress(id);

        if (progress == null) {
            reject(MembershipChangeDropReason.NO_LEADER);
            return false;
        }

        if (l.isLeaderTransferInProgress()) {
            reject(MembershipChangeDropReason.LEADER_TRANSFER_IN_PROGRESS);
            return false;
        }

        if (!l.canAcceptMembershipChange(log.applied())) {
            reject(MembershipChangeDropReason.MEMBERSHIP_CHANGE_PENDING);
            return false;
        }

        return true;
    }

    /**
     * Appends a membership change entry to the log and broadcasts it.
     *
     * <p>If the append succeeds (entry fits within the uncommitted
     * size limit), the leader records the entry's index as the pending
     * membership change index — blocking further config proposals
     * until this one is applied — and broadcasts the new entry to all
     * peers.</p>
     *
     * <p>If the append fails (uncommitted size exceeded), the proposal
     * is dropped without side effects.</p>
     *
     * @param l                     the leader state
     * @param entry                 the config change entry to append
     * @param membershipChangeIndex the log index of this entry
     */
    private void appendMembershipEntry(Leader l, Entry entry, long membershipChangeIndex) throws StorageException {
        var success = appendEntries(l, List.of(entry));

        if (success) {
            l.membershipChange(membershipChangeIndex);
            broadcastAppend(l);
        } else {
            reject(MembershipChangeDropReason.EXCEEDS_UNCOMMITTED_SIZE);
        }
    }

    // ────────────────────── MEMBERSHIP CHANGE — APPLY (COMMIT-TIME) ──────────────────────

    /**
     * Handles a committed membership change fed back from the engine
     * via {@link Message.ApplyMembershipChange}.
     *
     * <p>Two steps: (1) apply the change to the membership config via
     * {@link #applyMembershipChange}, (2) trigger role-specific side
     * effects via {@link #switchMembership} — leader reconciles
     * progress, follower/learner checks for promotion/demotion.</p>
     *
     * <p>For Candidate/PreCandidate the config is updated but no
     * role switch occurs — the election outcome will determine the
     * next role.</p>
     *
     * @param l   the current leader role (used for progress reconciliation)
     * @param amc the committed membership change message
     */
    private void handleApplyMembershipChange(Leader l, Message.ApplyMembershipChange amc) throws StorageException {
        applyMembershipChange(amc.changes());
        switchMembership(l);
    }

    /**
     * @param f   the current follower role (used for demotion check)
     * @param amc the committed membership change message
     * @see #handleApplyMembershipChange(Leader, Message.ApplyMembershipChange)
     */
    private void handleApplyMembershipChange(Follower f, Message.ApplyMembershipChange amc) throws StorageException {
        applyMembershipChange(amc.changes());
        switchMembership(f);
    }

    /**
     * @param l   the current learner role (used for promotion check)
     * @param amc the committed membership change message
     * @see #handleApplyMembershipChange(Leader, Message.ApplyMembershipChange)
     */
    private void handleApplyMembershipChange(Learner l, Message.ApplyMembershipChange amc) throws StorageException {
        applyMembershipChange(amc.changes());
        switchMembership(l);
    }

    /**
     * Candidate/PreCandidate: updates config only, no role switch.
     *
     * @param amc the committed membership change message
     */
    private void handleApplyMembershipChange(Message.ApplyMembershipChange amc) throws StorageException {
        applyMembershipChange(amc.changes());
    }

    /**
     * Handles a committed leave-joint entry fed back from the engine
     * via {@link Message.ApplyLeaveJoint}.
     *
     * <p>Same pattern as
     * {@link #handleApplyMembershipChange(Leader, Message.ApplyMembershipChange)}:
     * apply the config change then trigger role-specific side effects.
     * After leave-joint, the config is no longer joint and a new
     * membership change can be proposed.</p>
     *
     * @param l the current leader role (used for progress reconciliation)
     */
    private void handleApplyLeaveJoint(Leader l) throws StorageException {
        applyLeaveJoint();
        switchMembership(l);
    }

    /**
     * @param f the current follower role (used for demotion check)
     * @see #handleApplyLeaveJoint(Leader)
     */
    private void handleApplyLeaveJoint(Follower f) throws StorageException {
        applyLeaveJoint();
        switchMembership(f);
    }

    /**
     * @param l the current learner role (used for promotion check)
     * @see #handleApplyLeaveJoint(Leader)
     */
    private void handleApplyLeaveJoint(Learner l) throws StorageException {
        applyLeaveJoint();
        switchMembership(l);
    }

    /** Candidate/PreCandidate: applies leave-joint config only, no role switch. */
    private void handleApplyLeaveJoint() throws StorageException {
        applyLeaveJoint();
    }

    /**
     * Applies a membership change entry that has been committed.
     *
     * <p>Creates a short-lived {@link MembershipChanger} with the
     * current config, executes the protocol (which decides simple vs.
     * joint), and switches to the resulting config. Called on
     * <b>every</b> node — leader, follower, learner — when the entry
     * is applied to the state machine.</p>
     *
     * @param mc the membership changes from the committed entry
     */
    private void applyMembershipChange(MembershipChanges mc) throws StorageException {
        var changer = new MembershipChanger(membership);
        membership = changer.executeProtocol(mc);
    }

    /**
     * Applies a leave-joint entry that has been committed.
     *
     * <p>Drops the outgoing voter set and promotes the incoming set as
     * the sole config. nextLearners are moved to learners. After this,
     * the config is no longer joint.</p>
     */
    private void applyLeaveJoint() throws StorageException {
        var changer = new MembershipChanger(membership);
        membership = changer.leaveJoint();
    }



    /**
     * Leader-specific side effects after a membership config change.
     *
     * <p>This method handles five concerns in order:</p>
     *
     * <ol>
     *   <li><b>Progress reconciliation</b>: delegates to
     *       {@link Leader#applyNewMembership} which rebuilds the
     *       progress map to match the new config — adding new peers,
     *       updating roles, and dropping removed peers.</li>
     *   <li><b>Self-removal check</b>: if the leader is no longer a
     *       member (e.g. removed in a simple change), it steps down to
     *       follower immediately. It does not campaign again.</li>
     *   <li><b>Self-demotion check</b>: if the leader was demoted to
     *       learner (e.g. during leave-joint when it was in
     *       nextLearners), it transitions to the learner role. Learners
     *       cannot lead.</li>
     *   <li><b>Commit advancement</b>: the quorum may have changed
     *       (e.g. a three-node cluster becomes five-node), so the
     *       committed index is recalculated. If it advances, new
     *       entries are broadcast. If not, probes are sent to newly
     *       added peers so they can catch up.</li>
     *   <li><b>Transfer abort</b>: if a leadership transfer was in
     *       progress and the transferee is no longer a voter (removed
     *       or demoted), the transfer is aborted — a non-voter cannot
     *       become leader.</li>
     * </ol>
     */
    private void switchMembership(Leader l) throws StorageException {
        l.applyNewMembership(membership, log.lastIndex());

        if (!membership.isMember(id)) {
            becomeFollower(term);
            return;
        }
        if (membership.isLearner(id)) {
            becomeLearner(term);
            return;
        }

        if (log.tryCommit(term, membership.majorityAgreed(l.matchIndexer()))) {
            broadcastAppend(l);
        } else {
            broadcastProbe(l);
        }

        if (l.transferTarget().map(id -> !membership.isVoter(id)).orElse(false)) {
            l.abortLeaderTransfer();
        }
    }

    /**
     * Follower-specific side effects: transitions to learner if
     * demoted.
     *
     * <p>A follower that is now a learner in the new config transitions
     * to the {@link Learner} role. It retains its known leader ID so
     * it can continue forwarding proposals.</p>
     */
    private void switchMembership(Follower f) {
        if (membership.isLearner(id)) {
            becomeLearner(term, f.leaderId());
        }
    }

    /**
     * Learner-specific side effects: transitions to follower if
     * promoted.
     *
     * <p>A learner that is now a voter in the new config transitions
     * to the {@link Follower} role. As a follower, it gains the
     * ability to campaign and participate in elections.</p>
     */
    private void switchMembership(Learner l) {
        if (membership.isVoter(id)) {
            becomeFollower(term, l.leaderId());
        }
    }

    // ────────────────────── LINEARIZABLE READS ──────────────────────

    /**
     * Responds to a confirmed read index request.
     *
     * <p>Routes the response based on who requested the read:</p>
     *
     * <ul>
     *   <li><b>Local (from == this node):</b> add to
     *       {@code readsAwaitingApply} — released to the output
     *       buffer once the applied index catches up.</li>
     *   <li><b>Remote (a follower forwarded the read):</b> send a
     *       {@code ReadIndexResponse} message back. The follower then
     *       adds the read state to its own output.</li>
     * </ul>
     *
     * @param from      the node that originated the read request
     * @param readIndex the committed index at which the read is safe
     *                  to serve
     */
    private void respondToReadIndex(NodeId from, long readIndex) {
        if (from.equals(id)) {
            readsAwaitingApply.add(new ReadState(readIndex));
            releaseAppliedReads();
            return;
        }
        send(new Message.ReadIndexResponse(from, id, term, readIndex));
    }

    /**
     * Returns true if the leader has committed at least one entry in
     * the current term.
     *
     * <p>A newly elected leader inherits the previous leader's commit
     * index, which may not be the highest possible. Until the leader
     * commits its own entry (typically a no-op appended at election),
     * its commit index is not authoritative. Serving reads before this
     * point could return stale data.</p>
     *
     * <p>Implementation: checks if the term of the entry at the
     * committed index equals the current term. If so, at least one
     * current-term entry has been committed.</p>
     */
    private boolean committedInCurrentTerm() {
        try {
            return term == log.term(log.committed());
        } catch (StorageException e) {
            return false;
        }
    }

    /**
     * Replays deferred read index requests once the leader's first
     * commit in the current term is detected.
     *
     * <p>Called on the commit path (when {@code tryCommit} advances
     * the commit index). If {@code committedInCurrentTerm()} is now
     * true, drains the deferred messages from {@link Leader} and
     * reprocesses each one through
     * {@link #handleReadIndex(Leader, Message.ReadIndex)} — which will
     * now proceed past the deferral check and register them as pending
     * reads.</p>
     *
     * <p>This is a one-time operation per term: after draining, the
     * leader's deferred list becomes immutable-empty, and subsequent
     * reads go directly through the normal path.</p>
     */
    private void releaseDeferredReadIndex(Leader l) {
        if (!committedInCurrentTerm()) {
            return;
        }

        for (var ri : l.drainDeferredReadIndex()) {
            handleReadIndex(l, ri);
        }
    }

    /**
     * Handles a read index request on the Leader.
     *
     * <p>Three preconditions are checked in order:</p>
     *
     * <ol>
     *   <li><b>Singleton cluster:</b> the leader is trivially the only
     *       voter. No heartbeat confirmation needed — respond
     *       immediately with the current commit index.</li>
     *   <li><b>Not committed in current term:</b> the leader's commit
     *       index may be stale. Defer the request until the first
     *       commit happens (see {@link Leader#deferReadIndex}).</li>
     *   <li><b>Normal path:</b> register a pending read at the current
     *       commit index and confirm leadership per the configured
     *       mode:
     *       <ul>
     *         <li>{@code LEASE}: respond immediately — the leader
     *             trusts its lease (quorum was contacted within the
     *             election timeout window).</li>
     *         <li>{@code HEARTBEAT_TIMEOUT}: register the read and
     *             wait for the next periodic heartbeat to carry the
     *             confirmation.</li>
     *         <li>{@code HEARTBEAT_IMMEDIATE}: register the read and
     *             broadcast a heartbeat right away for lowest
     *             latency.</li>
     *       </ul>
     *       </li>
     * </ol>
     *
     * <p>Note: the {@code readIndex} stored for each pending read is
     * {@code log.committed()} at registration time — not the index the
     * application asked for. The leader guarantees that all entries up
     * to this index are committed and will not be overwritten.</p>
     */
    private void handleReadIndex(Leader l, Message.ReadIndex ri) {
        if (membership.isSingleton()) {
            respondToReadIndex(ri.from(), log.committed());
            return;
        }

        if (!committedInCurrentTerm()) {
            l.deferReadIndex(ri);
            return;
        }

        switch (config.readIndexMode()) {
            case ReadIndexMode.LEASE -> respondToReadIndex(ri.from(), log.committed());
            case ReadIndexMode.HEARTBEAT_TIMEOUT -> l.addPendingReadIndex(ri.from(), log.committed());
            case ReadIndexMode.HEARTBEAT_IMMEDIATE -> {
                l.addPendingReadIndex(ri.from(), log.committed());
                broadcastHeartbeat(l);
            }
        }

    }

    /**
     * Handles a read index request on a Follower.
     *
     * <p>Followers cannot serve linearizable reads on their own — they
     * don't know if the leader's commit index is authoritative. The
     * request is forwarded to the leader, which will confirm its
     * authority and respond via {@code ReadIndexResponse}.</p>
     *
     * <p>If the follower doesn't know the leader (e.g., during an
     * election or after a restart before receiving a heartbeat), the
     * request is dropped and the application should be notified so it
     * can retry or return an error to the client.</p>
     */
    private void handleReadIndex(Follower f, Message.ReadIndex ri) {
        if (!f.hasLeader()) {
            reject(ReadDropReason.NO_LEADER);
            return;
        }

        send(new Message.ReadIndex(f.leaderId(), id));
    }

    /**
     * Handles a read index request on a Learner.
     *
     * <p>Same as Follower: learners forward the request to the leader.
     * Learners are non-voting members — they cannot confirm leadership
     * themselves.</p>
     */
    private void handleReadIndex(Learner l, Message.ReadIndex ri) {
        if (!l.hasLeader()) {
            reject(ReadDropReason.NO_LEADER);
            return;
        }
        send(new Message.ReadIndex(l.leaderId(), id));
    }

    /**
     * Handles a read index response received by a Follower or Learner.
     *
     * <p>The leader confirmed its authority and is telling this node
     * that the read is safe at the given index. The node adds it to
     * {@code readsAwaitingApply} — it will be surfaced in the output
     * once the local applied index reaches the read index.</p>
     */
    private void handleReadIndexResponse(Message.ReadIndexResponse rir) {
        readsAwaitingApply.add(new ReadState(rir.readIndex()));
        releaseAppliedReads();
    }


    /**
     * Moves confirmed reads to the output buffer once the state machine
     * has caught up.
     *
     * <p>Called from two trigger points: (1) when a read is first
     * confirmed (heartbeat majority or leader response) — in case
     * applied already covers the index, and (2) when
     * {@code appliedTo} advances the applied watermark. The queue is
     * FIFO-ordered by non-decreasing index, so draining stops at the
     * first read whose index exceeds applied.</p>
     */
    private void releaseAppliedReads() {
        while (!readsAwaitingApply.isEmpty() && readsAwaitingApply.peek().index() <= log.applied()) {
            readStates.add(readsAwaitingApply.poll());
        }
    }

    // ────────────────────── FORGET LEADER ──────────────────────

    /**
     * Handles ForgetLeader on a Follower — clears the known leader,
     * making this node a leaderless follower in the same term.
     *
     * <p>The node stays in the current term and does not campaign. It
     * will:</p>
     *
     * <ul>
     *   <li>Grant pre-votes/votes immediately (no longer rejects based
     *       on "recently heard from leader")</li>
     *   <li>Revert to a normal follower if it hears from the leader
     *       again (via heartbeat or AppendEntries)</li>
     *   <li>Campaign normally when its election timeout fires</li>
     * </ul>
     *
     * <p><b>Blocked with lease-based reads.</b> ForgetLeader's purpose
     * is to bypass the vote rejection that protects the leader's
     * lease. Lease reads depend on that rejection to guarantee no new
     * leader is elected during the lease window. Allowing ForgetLeader
     * would let a new leader be elected while the old leader still
     * serves lease reads — violating linearizability. These two
     * mechanisms are fundamentally incompatible: ForgetLeader exists
     * to skip the timeout, but lease safety requires honoring it.</p>
     */
    private void forgetLeader(Follower f) {
        if (config.readIndexMode() == ReadIndexMode.LEASE) {
            return;
        }
        f.forgetLeader();
    }

    /**
     * Handles ForgetLeader on a Learner — same logic as Follower.
     *
     * <p>Learners also track a leader reference (for forwarding
     * proposals and read requests). Forgetting the leader lets the
     * learner participate in fast elections by granting votes
     * immediately.</p>
     */
    private void forgetLeader(Learner l) {
        if (config.readIndexMode() == ReadIndexMode.LEASE) {
            return;
        }
        l.forgetLeader();
    }

    // ────────────────────── STORAGE ACKNOWLEDGMENTS ──────────────────────

    /**
     * Advances the log's applied watermark.
     *
     * <p>Takes the max of the current applied index and the new index to
     * guard against out-of-order acks (possible with async storage). The
     * log uses this to update backpressure tracking for applying entries.</p>
     *
     * @param index the highest log index the application has applied
     * @param size  the cumulative byte size of applied entries
     */
    private void appliedTo(long index, long size) {
        var applied = Math.max(log.applied(), index);
        log.appliedTo(applied, size);
        releaseAppliedReads();
    }

    /**
     * Leader-specific applied-to: advances the watermark and triggers
     * auto-leave-joint if conditions are met.
     *
     * <p>When the transition type is {@code JOINT_AUTO}, the leave-joint
     * phase should be proposed automatically once the enter-joint entry
     * has been applied. Three conditions must all hold:</p>
     * <ul>
     *   <li>The config is in joint consensus ({@code isJoint()})</li>
     *   <li>The transition type is {@code JOINT_AUTO}</li>
     *   <li>The applied index has caught up to the config change index
     *       ({@code canAcceptMembershipChange} returns true)</li>
     * </ul>
     */
    private void appliedTo(Leader l, long index, long size) {
        appliedTo(index, size);
        if (membership.isJoint() && membership.transition() == MembershipTransition.JOINT_AUTO && l.canAcceptMembershipChange(index)) {
            try {
                step(new Message.LeaveJointProposal(id, id));
            } catch (StorageException s) {
                // Storage failure during auto-leave. The cluster remains in
                // joint consensus. The next appliedTo call will retry.
            }
        }
    }

    /**
     * Acknowledges a persisted snapshot: clears it from the unstable log
     * and advances the applied watermark to the snapshot's index.
     *
     * <p>A snapshot represents fully applied committed state up to its index,
     * so both the persistence ack ({@code stableSnapshotTo}) and the apply
     * ack ({@code appliedTo}) happen in one step.</p>
     *
     * <p><b>Not term-gated.</b> Snapshots carry committed state which is
     * immutable across term changes — safe to apply regardless of whether
     * the term changed since the persist was requested.</p>
     *
     * @param snapshot the snapshot that was persisted
     */
    private void appliedSnapshot(Snapshot snapshot) {
        log.stableSnapshotTo(snapshot.index());
        appliedTo(snapshot.index(), 0);
    }

    /**
     * Leader-aware variant: also triggers auto-leave-joint check after
     * advancing the applied watermark.
     *
     * @param l        the current leader state
     * @param snapshot the snapshot that was persisted
     */
    private void appliedSnapshot(Leader l, Snapshot snapshot) {
        log.stableSnapshotTo(snapshot.index());
        appliedTo(l, snapshot.index(), 0);
    }


    /**
     *
     * Attempts to mark persisted entries as stable in the unstable log.
     *
     * <p>Two guards protect against incorrect truncation:</p>
     * <ul>
     *   <li>{@code logIndex != 0} — zero means no entry info (only a hard
     *       state update or snapshot). Nothing to stabilize.</li>
     *   <li>{@code term == currentTerm} — ABA protection. If the term changed,
     *       the entries at these indexes may have been overwritten by a new
     *       leader. Truncating based on a stale ack causes an inconsistent
     *       view between the unstable log and stable storage.</li>
     * </ul>
     *
     * <h4>ABA example — stale persistence ack after leadership changes</h4>
     *
     * <p>Cluster: {A, B, C, D, E}. A is leader (term 2). Focus on B
     * (follower) and its async persistence pipeline.</p>
     *
     * <pre>
     *   All nodes start with committed log: [(idx=1, term=1)]
     *
     *   Step  Event                                B's term  B's unstable                  Disk write pipeline
     *   ----  -----                                --------  ------------                  --------------------
     *   1.    A (leader, term 2) proposes.         2         [(idx=2, term=2),             WRITING [(idx=2, term=2), (idx=3, term=2)]
     *         B receives, starts async persist.               (idx=3, term=2)]
     *
     *   2.    A crashes. C gets votes from D, E.   3         [(idx=2, term=2),             WRITING [(idx=2, term=2), (idx=3, term=2)]
     *         C becomes leader (term 3).                      (idx=3, term=2)]
     *
     *   3.    C proposes at SAME indexes, term 3.  3         [(idx=2, term=3),             WRITING [(idx=2, term=2), (idx=3, term=2)]
     *         B receives, overwrites unstable.                (idx=3, term=3)]             QUEUED  [(idx=2, term=3), (idx=3, term=3)]
     *
     *   4.    C crashes. A gets votes from D, E.   4         [(idx=2, term=3),             WRITING [(idx=2, term=2), (idx=3, term=2)]
     *         A becomes leader (term 4).                      (idx=3, term=3)]             QUEUED  [(idx=2, term=3), (idx=3, term=3)]
     *
     *   5.    A sends original entries to B.       4         [(idx=2, term=2),             WRITING [(idx=2, term=2), (idx=3, term=2)]
     *         B overwrites unstable again.                    (idx=3, term=2)]             QUEUED  [(idx=2, term=3), (idx=3, term=3)]
     *         Same (idx, term) pairs as step 1!                                            QUEUED  [(idx=2, term=2), (idx=3, term=2)]
     *
     *   6.    Step 1's write completes.            4         [(idx=2, term=2),             QUEUED  [(idx=2, term=3), (idx=3, term=3)]
     *         LogPersisted(term=2, logIndex=3,                (idx=3, term=2)]             QUEUED  [(idx=2, term=2), (idx=3, term=2)]
     *                      logTerm=2) arrives.
     * </pre>
     *
     * <h4>Without term check</h4>
     * <ol>
     *   <li>LogPersisted says (logIndex=3, logTerm=2).</li>
     *   <li>B's current unstable last entry is (idx=3, term=2) — MATCHES.</li>
     *   <li>stableTo succeeds, unstable truncated (entries removed from memory).</li>
     *   <li>The queued write [(idx=2, term=3), (idx=3, term=3)] executes next
     *       and overwrites disk.</li>
     *   <li>Now B's unstable is empty and disk has (idx=2, term=3), (idx=3, term=3).
     *       But the correct entries — (idx=2, term=2), (idx=3, term=2) from
     *       the current leader — are gone from memory (truncated) and not yet
     *       on disk (the last queued write hasn't run).</li>
     *   <li><b>INCONSISTENT</b>: B's unstable is empty and disk has the wrong
     *       entries. Any log read in this window sees stale data.</li>
     * </ol>
     *
     * <p><b>Inconsistency window</b> (after write #2 completes, before
     * write #3 completes):</p>
     * <ul>
     *   <li>B's unstable: empty</li>
     *   <li>B's disk: [(idx=2, t=3), (idx=3, t=3)]</li>
     *   <li>B's apparent last log entry: (idx=3, t=3)</li>
     *   <li>B's correct last log entry: (idx=3, t=2)</li>
     * </ul>
     *
     * <p><b>Safety violation — state machine divergence:</b></p>
     * <ol>
     *   <li>A (term 4 leader) commits — no-op at idx=4 reaches majority.
     *       B also has entry 4 in unstable.</li>
     *   <li>Write #2 completes — disk overwrites to
     *       [(idx=2, t=3), (idx=3, t=3)].</li>
     *   <li>A sends heartbeat with leaderCommit=4 — reaches B and some
     *       nodes, but not all.</li>
     *   <li>B applies entries 2-3 from log — unstable is empty, falls
     *       through to disk — gets C's uncommitted term 3 entries.</li>
     *   <li>Applying watermark advances — entries 2-3 are never re-applied
     *       on B.</li>
     *   <li>Write #3 eventually corrects B's disk, but B's state machine
     *       already consumed the wrong data.</li>
     * </ol>
     * <p><b>Result:</b> B's state machine permanently diverges from the
     * rest of the cluster.</p>
     *
     * <p><b>Safety violation — cascading divergence if B becomes leader:</b></p>
     * <ol>
     *   <li>A crashes. B wins election at term 5 — either because B has
     *       entry 4 (idx=4, t=4) matching other nodes, or because B's
     *       inflated last term (3) wins votes over others' (2).</li>
     *   <li>B appends no-op at idx=5 (term=5).</li>
     *   <li>B replicates its log. Entries 2-3 read from disk → wrong
     *       (t=3). Overwrites followers' correct entries.</li>
     *   <li>Nodes that received leaderCommit from A already applied correct
     *       entries. Applied watermark prevents reapply — but their logs
     *       and state machines are now inconsistent.</li>
     *   <li>Nodes that did NOT receive leaderCommit accept B's wrong
     *       entries. When B sends leaderCommit, they apply wrong entries
     *       — state machines diverge.</li>
     *   <li>Write #3 eventually corrects B's disk, but B already
     *       replicated the wrong entries.</li>
     * </ol>
     * <p><b>Result:</b> Mixed corruption across the cluster — B and some
     * nodes have wrong state machines, other nodes have correct state
     * machines but wrong logs. This requires write #3 (the correct term 4
     * entries) to take longer than the election timeout — possible with
     * batched writes or a slow downstream storage system.</p>
     *
     * <p><b>Safety violation — incorrect vote rejection:</b></p>
     * <ol>
     *   <li>A (term 4 leader) crashes before sending no-op at idx=4 to B.
     *       B's last entry is (idx=3, t=3) from disk — inflated.</li>
     *   <li>D starts election at term 5. D's log is correct: last entry
     *       (idx=3, t=2).</li>
     *   <li>D sends RequestVote to B. Vote comparison: higher last term
     *       wins, if tied higher index wins.</li>
     *   <li>B sees its last term as 3 (wrong), D's last term is 2 —
     *       rejects D as less up-to-date.</li>
     *   <li>Correct last terms are equal (both 2), equal indexes (both 3)
     *       — B should grant.</li>
     * </ol>
     * <p><b>Result:</b> If B's vote was needed for majority (3 of 5), D's
     * election fails — availability loss. B's inflated last term makes it
     * reject candidates that are equally or more qualified.</p>
     *
     * <h4>With term check</h4>
     * <ol>
     *   <li>LogPersisted(term=2) arrives. term (2) != B's current term (4)
     *       → entry stability IGNORED. Unstable stays intact.</li>
     *   <li>Queued write [(idx=2, term=3), (idx=3, term=3)] completes.
     *       LogPersisted(term=3) arrives. term (3) != B's current term (4)
     *       → IGNORED again. Unstable still intact.</li>
     *   <li>Queued write [(idx=2, term=2), (idx=3, term=2)] completes.
     *       LogPersisted(term=4) arrives. term (4) == B's current term (4)
     *       → stableTo succeeds. Disk and unstable are now consistent.</li>
     * </ol>
     *
     * <p>When both guards pass, delegates to {@code log.stableTo(logTerm, logIndex)}
     * which verifies the term at that index still matches in the unstable log
     * (a second safety net inside UnstableLog) before truncating.</p>
     *
     * @param lp the persistence acknowledgment to process
     */
    private void tryStableTo(Message.LogPersisted lp) {
        if (lp.logIndex() != 0 && lp.term() == term) {
            log.stableTo(lp.logTerm(), lp.logIndex());
        }
    }

    /**
     * Handles LogPersisted as leader — entry stability (term-gated) and
     * snapshot application with auto-leave-joint check.
     *
     * @param l  the current leader state
     * @param lp the persistence acknowledgment
     */
    private void handleLogPersisted(Leader l, Message.LogPersisted lp) {
        tryStableTo(lp);
        lp.snapshot().ifPresent(s -> this.appliedSnapshot(l, s));
    }

    /**
     * Handles LogPersisted for non-leader roles — no auto-leave-joint.
     *
     * @param lp the persistence acknowledgment
     */
    private void handleLogPersisted(Message.LogPersisted lp) {
        tryStableTo(lp);
        lp.snapshot().ifPresent(this::appliedSnapshot);
    }

    /**
     * Handles AppliedToStateMachine as leader — advances applied watermark,
     * checks auto-leave-joint, and frees uncommitted entry quota.
     *
     * @param l   the current leader state
     * @param asm the state machine apply acknowledgment
     */
    private void handleAppliedToStateMachine(Leader l, Message.AppliedToStateMachine asm) {
        if (asm.entries().isEmpty()) {
            return;
        }

        var size = Entry.calculateSize(asm.entries());
        appliedTo(l, asm.entries().getLast().index(), size);
        l.decreaseUncommittedSize(size);
    }

    /**
     * Handles AppliedToStateMachine for non-leader roles — watermark only.
     *
     * @param asm the state machine apply acknowledgment
     */
    private void handleAppliedToStateMachine(Message.AppliedToStateMachine asm) {
        if (asm.entries().isEmpty()) {
            return;
        }

        var size = Entry.calculateSize(asm.entries());
        appliedTo(asm.entries().getLast().index(), size);
    }

}
