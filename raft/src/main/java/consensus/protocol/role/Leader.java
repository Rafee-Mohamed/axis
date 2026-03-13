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

public final class Leader implements Role {
    private long uncommittedSize;
    private Optional<NodeId> transferTarget;
    private long membershipChangeIndex;
    private final TickTimer quorumCheckTimer;
    private final TickTimer heartbeatTimer;
    private final LeaderConfig config;
    private final ClusterProgress clusterProgress;
    /**
     * Tracks pending linearizable read requests and heartbeat sequence acks.
     * Created fresh with each leader election (no state carries over from
     * previous terms — old pending reads are implicitly abandoned).
     */
    private final ReadIndex readIndex;

    /**
     * Read index requests that arrived before the leader committed an entry
     * in its current term.
     *
     * <p>A newly elected leader's commit index may be stale — it reflects the
     * previous leader's last known commit. Until the leader commits at least
     * one entry in its own term (typically a no-op), it cannot guarantee that
     * its commit index is the highest possible. Serving reads before this
     * point could return stale data.</p>
     *
     * <p>These messages are replayed via {@link #drainDeferredReadIndex()} once
     * the leader's first entry is committed. After draining, this field is
     * replaced with an immutable empty list — no further deferrals occur in
     * the same term since {@code committedInCurrentTerm()} is now true.</p>
     */
    private List<Message.ReadIndex> deferredReadIndexMessages;

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

    public RoleType type() {
        return RoleType.LEADER;
    }

    public boolean canCheckQuorumAfterTick() {
        if (config.leaderLivenessPolicy() != LeaderLivenessPolicy.QUORUM_VERIFIED)
            return false;

        return quorumCheckTimer.resetIfTimedOutAfterTick();
    }

    public boolean canSendHeartBeatAfterTick() {
        return heartbeatTimer.resetIfTimedOutAfterTick();
    }

    public void abortLeaderTransfer() {
        transferTarget = Optional.empty();
    }

    public boolean tryIncreaseUncommittedSize(List<Entry> entries) {
        var size = Entry.calculateSize(entries);
        if ((uncommittedSize + size) > config.maxUncommittedSize())
            return false;

        uncommittedSize += size;
        return true;
    }

    public void decreaseUncommittedSize(long size) {
        uncommittedSize = Math.max(0, uncommittedSize - size);
    }

    public ClusterProgress clusterProgress() {
        return clusterProgress;
    }



    public boolean isLeaderTransferee(NodeId id) {
        return transferTarget.map(id::equals).orElse(false);
    }

    public boolean isLeaderTransferInProgress() {
        return transferTarget.isPresent();
    }

    public boolean hasProgress(NodeId id) {
        return clusterProgress.hasProgress(id);
    }

    public PeerProgress progress(NodeId id) {
        return clusterProgress.progress(id);
    }


    public Set<NodeId> peers() {
        return clusterProgress.peers();
    }



    public Optional<NodeId> transferTarget() {
        return transferTarget;
    }

    // Implicitly aborts any previous in-progress transfer to a different target.
    public void transferLeadership(NodeId transferee) {
        transferTarget = Optional.of(transferee);
        // Reset to give the transferee a full election-timeout window to catch up.
        quorumCheckTimer.reset();
    }

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
     * that index — signaled by {@code index >= membershipChangeIndex}.</p>
     *
     * <p>This gate prevents overlapping config changes, which could violate
     * the single-membership-change-at-a-time invariant in Raft.</p>
     *
     * @param index the current applied index (from {@code log.applied()})
     * @return true if the last config change has been applied and a new one
     *         can be proposed
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
     * <p>Called by Raft when a read request arrives and
     * {@code committedInCurrentTerm()} is false. The message is buffered here
     * and replayed later.</p>
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
     * replaced with an immutable empty list as a signal — any future call to
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
     *
     * <p>Delegates to {@link ReadIndex#nextSeq()}. The returned value is
     * attached to the heartbeat message broadcast to all peers. Followers
     * echo it in their heartbeat response, enabling the {@link ReadIndex}
     * to correlate acks to specific heartbeat rounds.</p>
     *
     * @return the new sequence number for the outgoing heartbeat
     */
    public long nextHeartbeatSeq() {
        return readIndex.nextSeq();
    }

    /**
     * Records a heartbeat ack and drains any newly confirmed pending reads.
     *
     * <p>Combines two operations atomically:</p>
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
     *
     * <p>The read is associated with the next heartbeat seq ({@code seq + 1})
     * inside {@link ReadIndex#addPending}, ensuring it is only confirmed by
     * heartbeats sent after this registration.</p>
     *
     * @param from           the node requesting the read (leader itself or a
     *                       follower forwarding a client read)
     * @param committedIndex the leader's current commit index, which becomes
     *                       the read index returned to the application
     */
    public void addPendingReadIndex(NodeId from, long committedIndex) {
        readIndex.addPending(from, committedIndex);
    }


    public Map<NodeId, Boolean> getQuorumVotesAndDeactivate() {
        return clusterProgress.getQuorumVotesAndDeactivate();
    }

    public Function<NodeId, OptionalLong> matchIndexer() {
        return clusterProgress.matchIndexer();
    }

    public void applyNewMembership(MembershipConfig membership, long lastIndex) {
        clusterProgress.applyNewMembership(membership, lastIndex);
    }

}
