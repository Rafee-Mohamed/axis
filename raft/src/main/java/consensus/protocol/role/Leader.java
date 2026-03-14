package consensus.protocol.role;

import consensus.protocol.policy.LeaderLivenessPolicy;
import consensus.protocol.read.ReadIndex;
import consensus.cluster.progress.ClusterProgress;
import consensus.cluster.progress.PeerProgress;
import consensus.config.LeaderConfig;
import consensus.cluster.membership.MembershipConfig;
import consensus.core.NodeId;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.*;
import java.util.function.Function;

/**
 * The elected leader responsible for log replication and coordination.
 *
 * <p>The leader accepts client proposals, appends them to its log,
 * replicates entries to all peers via {@link ClusterProgress}, and
 * advances the commit index once a quorum acknowledges a given index.
 * It also drives heartbeats, linearizable reads, leadership transfers,
 * and membership changes.</p>
 *
 * <p>Key invariants:</p>
 * <ul>
 *   <li>At most one uncommitted membership change in the log at a time
 *       (guarded by {@link #canAcceptMembershipChange}).</li>
 *   <li>Linearizable reads are deferred until the leader commits in
 *       its current term, then served via heartbeat-ack quorum.</li>
 *   <li>Uncommitted entry size is bounded to prevent unbounded memory
 *       growth from fast proposers.</li>
 * </ul>
 */
public final class Leader implements Role {

    /** Cumulative byte size of entries appended but not yet committed. */
    private long uncommittedSize;

    /** Target of an ongoing leadership transfer, empty if none in progress. */
    private Optional<NodeId> transferTarget;

    /**
     * Log index of the most recently proposed membership change entry.
     * A new membership change is blocked until the application applies
     * entries up to (or past) this index, enforcing Raft's single
     * pending membership change invariant.
     */
    private long membershipChangeIndex;

    /**
     * Timer for periodic quorum liveness checks. Only ticked when
     * {@link LeaderLivenessPolicy#QUORUM_VERIFIED} is active. Also
     * reset during leadership transfers to give the transferee a
     * full election-timeout window.
     */
    private final TickTimer quorumCheckTimer;

    /** Timer for periodic heartbeat broadcasts to all peers. */
    private final TickTimer heartbeatTimer;

    /** Leader-specific configuration (timeouts, limits, policies). */
    private final LeaderConfig config;

    /** Replication progress tracker for all peers in the cluster. */
    private final ClusterProgress clusterProgress;

    /**
     * Tracks pending linearizable read requests and heartbeat sequence acks.
     * Created fresh with each leader election - no state carries over from
     * previous terms; old pending reads are implicitly abandoned.
     */
    private final ReadIndex readIndex;

    /**
     * Read index requests that arrived before the leader committed an entry
     * in its current term.
     *
     * <p>A newly elected leader's commit index may be stale - it reflects the
     * previous leader's last known commit. Until the leader commits at least
     * one entry in its own term (typically a no-op), it cannot guarantee that
     * its commit index is the highest possible. Serving reads before this
     * point could return stale data.</p>
     *
     * <p>These messages are replayed via {@link #drainDeferredReadIndex()} once
     * the leader's first entry is committed. After draining, this field is
     * replaced with an immutable empty list - no further deferrals occur in
     * the same term since {@code committedInCurrentTerm()} is now true.</p>
     */
    private List<Message.ReadIndex> deferredReadIndexMessages;

    /**
     * Creates a new leader role.
     *
     * @param progress     cluster-wide replication progress tracker
     * @param leaderConfig leader-specific configuration
     */
    public Leader(ClusterProgress progress, LeaderConfig leaderConfig) {
        config = leaderConfig;
        clusterProgress = progress;
        transferTarget = Optional.empty();
        membershipChangeIndex = progress.leaderProgress().next();
        uncommittedSize = 0;
        quorumCheckTimer = new TickTimer(config.electionTimeout());
        heartbeatTimer = new TickTimer(config.heartbeatTimeout());
        readIndex = new ReadIndex();
        deferredReadIndexMessages = new ArrayList<>();
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link RoleType#LEADER}
     */
    @Override
    public RoleType type() {
        return RoleType.LEADER;
    }

    /**
     * Ticks the quorum-check timer and returns {@code true} if it fired.
     * Only active when the leader liveness policy is
     * {@link LeaderLivenessPolicy#QUORUM_VERIFIED}; returns {@code false}
     * unconditionally otherwise.
     *
     * @return {@code true} if the quorum-check timer expired this tick
     */
    public boolean canCheckQuorumAfterTick() {
        if (config.leaderLivenessPolicy() != LeaderLivenessPolicy.QUORUM_VERIFIED)
            return false;

        return quorumCheckTimer.resetIfTimedOutAfterTick();
    }

    /**
     * Ticks the heartbeat timer and returns {@code true} if it fired,
     * indicating the leader should broadcast heartbeats to all peers.
     *
     * @return {@code true} if the heartbeat timer expired this tick
     */
    public boolean canSendHeartBeatAfterTick() {
        return heartbeatTimer.resetIfTimedOutAfterTick();
    }

    /**
     * Cancels any in-progress leadership transfer.
     */
    public void abortLeaderTransfer() {
        transferTarget = Optional.empty();
    }

    /**
     * Attempts to reserve space for new entries in the uncommitted budget.
     *
     * @param entries the entries to account for
     * @return {@code true} if the entries fit; {@code false} if accepting
     *         them would exceed the configured maximum, in which case the
     *         proposal should be rejected
     */
    public boolean tryIncreaseUncommittedSize(List<Entry> entries) {
        var size = Entry.calculateSize(entries);
        if ((uncommittedSize + size) > config.maxUncommittedSize())
            return false;

        uncommittedSize += size;
        return true;
    }

    /**
     * Releases committed entry bytes from the uncommitted budget.
     * Clamped to zero to tolerate rounding or accounting drift.
     *
     * @param size the byte size of newly committed entries
     */
    public void decreaseUncommittedSize(long size) {
        uncommittedSize = Math.max(0, uncommittedSize - size);
    }

    /**
     * Returns the cluster-wide replication progress tracker.
     *
     * @return the cluster progress, never {@code null}
     */
    public ClusterProgress clusterProgress() {
        return clusterProgress;
    }

    /**
     * Returns {@code true} if the given node is the current transfer target.
     *
     * @param id the node id to check
     * @return whether {@code id} is the active transferee
     */
    public boolean isLeaderTransferee(NodeId id) {
        return transferTarget.map(id::equals).orElse(false);
    }

    /**
     * Returns {@code true} if a leadership transfer is in progress.
     *
     * @return whether a transfer target is set
     */
    public boolean isLeaderTransferInProgress() {
        return transferTarget.isPresent();
    }

    /**
     * Delegates to {@link ClusterProgress#hasProgress(NodeId)}.
     *
     * @param id the node id to look up
     * @return {@code true} if progress is tracked for this node
     */
    public boolean hasProgress(NodeId id) {
        return clusterProgress.hasProgress(id);
    }

    /**
     * Delegates to {@link ClusterProgress#progress(NodeId)}.
     *
     * @param id the node id to look up
     * @return the peer's replication progress
     */
    public PeerProgress progress(NodeId id) {
        return clusterProgress.progress(id);
    }

    /**
     * Delegates to {@link ClusterProgress#peers()}.
     *
     * @return the set of all tracked peer ids
     */
    public Set<NodeId> peers() {
        return clusterProgress.peers();
    }

    /**
     * Returns the current transfer target, if any.
     *
     * @return the transferee's node id, or empty if no transfer is active
     */
    public Optional<NodeId> transferTarget() {
        return transferTarget;
    }

    /**
     * Initiates a leadership transfer to the given node, implicitly aborting
     * any previous in-progress transfer. The quorum-check timer is reset to
     * give the transferee a full election-timeout window to catch up.
     *
     * @param transferee the node to transfer leadership to
     */
    public void transferLeadership(NodeId transferee) {
        transferTarget = Optional.of(transferee);
        quorumCheckTimer.reset();
    }

    /**
     * Returns {@code true} if the quorum-check timer has elapsed without
     * being reset. Used to detect whether a leadership transfer has
     * stalled (transferee unresponsive).
     *
     * @return whether the quorum-check timer has timed out
     */
    public boolean isQuorumCheckTimedOut() {
        return quorumCheckTimer.isTimedOut();
    }

    /**
     * Returns {@code true} if a new membership change proposal can be accepted.
     *
     * <p>At most one unapplied config change may exist in the log at a time.
     * {@code membershipChangeIndex} tracks the log index of the most recently
     * proposed (but possibly not yet applied) config change. A new proposal is
     * allowed only once the application has applied entries up to (or past)
     * that index - signaled by {@code index >= membershipChangeIndex}.</p>
     *
     * <p>This gate prevents overlapping config changes, which could violate
     * the single-membership-change-at-a-time invariant in Raft.</p>
     *
     * @param index the current applied index (from {@code log.applied()})
     * @return {@code true} if the last config change has been applied and a
     *         new one can be proposed
     */
    public boolean canAcceptMembershipChange(long index) {
        return index >= membershipChangeIndex;
    }

    /**
     * Records the log index of a newly proposed membership change entry.
     *
     * <p>Called immediately after a membership change or leave-joint entry
     * is successfully appended to the log. Until the application applies
     * entries up to this index, {@link #canAcceptMembershipChange} will
     * return {@code false}, blocking further config change proposals.</p>
     *
     * @param index the log index of the appended config change entry
     */
    public void membershipChange(long index) {
        membershipChangeIndex = index;
    }

    /**
     * Defers a read index request until the leader commits in its current term.
     *
     * <p>Called when a read request arrives and {@code committedInCurrentTerm()}
     * is {@code false}. The message is buffered and replayed later.</p>
     *
     * @param ri the read index message to defer
     */
    public void deferReadIndex(Message.ReadIndex ri) {
        deferredReadIndexMessages.add(ri);
    }

    /**
     * Drains all deferred read index messages, returning them for reprocessing.
     *
     * <p>Called when the leader's first commit in the current term is detected.
     * After this point, no further messages will be deferred (the list is
     * replaced with an immutable empty list as a signal - any future call to
     * {@link #deferReadIndex} would throw, catching bugs).</p>
     *
     * @return the buffered messages to replay through the normal read path
     */
    public List<Message.ReadIndex> drainDeferredReadIndex() {
        var messages = deferredReadIndexMessages;
        deferredReadIndexMessages = List.of();
        return messages;
    }

    /**
     * Increments and returns the next heartbeat sequence number.
     * Delegates to {@link ReadIndex#nextSeq()}.
     *
     * @return the new sequence number for the outgoing heartbeat
     */
    public long nextHeartbeatSeq() {
        return readIndex.nextSeq();
    }

    /**
     * Records a heartbeat ack and drains any newly confirmed pending reads.
     *
     * <p>Combines two operations:</p>
     * <ol>
     *   <li>Records that {@code from} has acked heartbeat seq {@code ackedSeq}.</li>
     *   <li>Checks if the new ack pushes the majority-agreed seq past any
     *       pending read's required seq. If so, those reads are drained and
     *       returned for response.</li>
     * </ol>
     *
     * @param from     the peer that responded
     * @param ackedSeq the heartbeat seq echoed in the response
     * @param mc       current membership config for quorum calculation
     * @return list of confirmed pending reads (empty if none were confirmed)
     */
    public List<ReadIndex.Pending> drainAfterAck(NodeId from, long ackedSeq, MembershipConfig mc) {
        readIndex.onHeartbeatAck(from, ackedSeq);
        return readIndex.drainAcked(clusterProgress.leaderId(), mc);
    }

    /**
     * Registers a new pending read request at the given commit index.
     * Delegates to {@link ReadIndex#addPending(NodeId, long)}.
     *
     * @param from           the node requesting the read (leader itself or a
     *                       follower forwarding a client read)
     * @param committedIndex the leader's current commit index, which becomes
     *                       the read index returned to the application
     */
    public void addPendingReadIndex(NodeId from, long committedIndex) {
        readIndex.addPending(from, committedIndex);
    }

    /**
     * Collects quorum-check votes from all peers and deactivates them.
     * Delegates to {@link ClusterProgress#getQuorumVotesAndDeactivate()}.
     *
     * @return map of peer id to active status ({@code true} if the peer
     *         responded since the last check)
     */
    public Map<NodeId, Boolean> getQuorumVotesAndDeactivate() {
        return clusterProgress.getQuorumVotesAndDeactivate();
    }

    /**
     * Returns a function that maps a peer to its match index.
     * Delegates to {@link ClusterProgress#matchIndexer()}.
     *
     * @return the match index lookup function for quorum commit calculation
     */
    public Function<NodeId, OptionalLong> matchIndexer() {
        return clusterProgress.matchIndexer();
    }

    /**
     * Applies a new membership configuration, adjusting the tracked
     * peer set (adding new members, removing departed ones).
     * Delegates to {@link ClusterProgress#applyNewMembership(MembershipConfig, long)}.
     *
     * @param membership the new membership configuration
     * @param lastIndex  the last log index (used to initialize new peers)
     */
    public void applyNewMembership(MembershipConfig membership, long lastIndex) {
        clusterProgress.applyNewMembership(membership, lastIndex);
    }

}
