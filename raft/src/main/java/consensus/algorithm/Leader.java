package consensus.algorithm;

import consensus.membership.MembershipConfig;
import consensus.message.Message;
import consensus.node.NodeId;
import consensus.storage.Entry;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class Leader implements Role {
    private Map<NodeId, PeerProgress> peerProgress;
    private final NodeId id;
    private Set<NodeId> peers;
    private final Function<NodeId, OptionalLong> matchIndexer;
    private long uncommittedSize;
    private final long maxUncommittedSize;
    private Optional<NodeId> transferTarget;
    private long membershipChangeIndex;
    private final boolean checkQuorum;
    private final TickTimer quorumCheckTimer;
    private final TickTimer heartbeatTimer;
    private final Inflight.Config inflightConfig;
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

    public Leader(NodeId leaderId, Set<NodeId> peers, long lastIndex, Inflight.Config inflightConfig) {
        id = leaderId;
        peerProgress = peers.stream().collect(Collectors.toMap((id) -> id, (_) -> new PeerProgress(inflightConfig, RoleType.VOTER)));
        // Leader's own progress starts in Replicate state and is always active.
        var leaderProgress = peerProgress.computeIfAbsent(leaderId, (_) -> new PeerProgress(inflightConfig, RoleType.VOTER));
        leaderProgress.becomeReplicate();
        leaderProgress.setActive(true);
        // Pre-compute the set of peers excluding ourselves for broadcast loops.
        this.peers = peers.stream().filter(Predicate.not(leaderId::equals)).collect(Collectors.toUnmodifiableSet());
        matchIndexer = (NodeId id) -> {
            var progress = peerProgress.get(id);
            return progress == null ? OptionalLong.empty() : OptionalLong.of(progress.match());
        };
        transferTarget = Optional.empty();
        membershipChangeIndex = lastIndex;
        uncommittedSize = 0;
        maxUncommittedSize = 0;
        checkQuorum = false;
        quorumCheckTimer = new TickTimer(10);
        heartbeatTimer = new TickTimer(10);
        this.inflightConfig = inflightConfig;
        readIndex = new ReadIndex();
        deferredReadIndexMessages = new ArrayList<>();
    }

    public Map<NodeId, PeerProgress> peerProgress() {
        return peerProgress;
    }

    /**
     * Reconciles the leader's peer progress map with a new membership config.
     *
     * <p>Called by the Raft layer after every membership change is applied to
     * the state machine — on both enter-joint and leave-joint transitions.
     * The leader must keep its progress map in sync with the config so that
     * replication, commit calculations, and quorum checks operate on the
     * correct set of peers.</p>
     *
     * <p>Reconciliation logic for each node in the new config:</p>
     * <ul>
     *   <li><b>New node (no existing progress)</b>: a fresh {@link PeerProgress}
     *       is created with {@code nextIndex = lastIndex + 1} (optimistic) and
     *       the appropriate role type. The leader will begin probing it.</li>
     *   <li><b>Existing node, role unchanged</b>: progress is carried over as-is.
     *       Replication state (match/next indices, inflight window) is preserved
     *       so there is no disruption.</li>
     *   <li><b>Existing node, promoted (learner → voter)</b>: the role type is
     *       upgraded. The node now participates in commit and election quorums.</li>
     *   <li><b>Existing node, demoted (voter → learner)</b>: the role type is
     *       downgraded. The node no longer counts toward quorum. Note: during
     *       joint consensus, a node in nextLearners is still a voter in the
     *       outgoing config — demotion only takes effect at leave-joint.</li>
     * </ul>
     *
     * <p>Nodes that were in the old config but are absent from the new config
     * are dropped — they don't appear in the new progress map. This handles
     * removed nodes cleanly.</p>
     *
     * <p>All existing and new peers are marked active, since a config change
     * is evidence of cluster liveness.</p>
     *
     * @param mc        the new membership config (already set on the Raft instance)
     * @param lastIndex the leader's last log index, used as the initial next
     *                  index for newly added peers
     */
    public void applyNewMembership(MembershipConfig mc, long lastIndex) {
        var members = mc.members();
        var newPeerProgress = new HashMap<NodeId, PeerProgress>(members.size());
        for (var member: members) {
            var existing = peerProgress.get(member);
            if (existing == null) {
                var roleType = mc.isVoter(member) ? RoleType.VOTER : RoleType.LEARNER;
                var newProgress = new PeerProgress(inflightConfig, roleType, lastIndex);
                newPeerProgress.put(member, newProgress);
            } else {
                if (mc.isLearner(member) && existing.isVoter()) {
                    existing.becomeLearner();
                } else if (mc.isVoter(member) && existing.isLearner()) {
                    existing.becomeVoter();
                }
                existing.setActive(true);
                newPeerProgress.put(member, existing);
            }
        }
        peerProgress = newPeerProgress;
        validateInvariants(mc);
        buildPeers();
    }

    /**
     * Rebuilds the broadcast peer set (all tracked peers excluding ourselves).
     */
    private void buildPeers() {
        var newPeers = new HashSet<>(peerProgress.keySet());
        newPeers.remove(id);
        peers = Collections.unmodifiableSet(newPeers);
    }

    /**
     * Validates that the peer progress map is consistent with the membership
     * config after reconciliation.
     *
     * <p>Four invariants are checked:</p>
     * <ol>
     *   <li><b>No missing progress</b>: every member in the config (voters,
     *       learners, nextLearners) must have a progress entry. A missing entry
     *       means the leader would silently stop replicating to that node.</li>
     *   <li><b>No orphan progress</b>: every progress entry must correspond to
     *       a config member. Orphan entries would waste resources and could
     *       corrupt quorum calculations.</li>
     *   <li><b>nextLearners tracked as voters</b>: nodes in nextLearners are
     *       still voters in the outgoing config (they only become learners at
     *       leave-joint). If their progress shows LEARNER, the demotion was
     *       applied too early and quorum calculation would be wrong.</li>
     *   <li><b>learners tracked as learners</b>: actual learners must not have
     *       VOTER progress — they must not count toward commit quorum.</li>
     * </ol>
     *
     * @param mc the membership config to validate against
     * @throws IllegalStateException if any invariant is violated
     */
    private void validateInvariants(MembershipConfig mc) {
        var members = mc.members();
        var membersWithNoProgress = members.stream()
            .filter(Predicate.not(peerProgress::containsKey))
            .collect(Collectors.toUnmodifiableSet());

        if (!membersWithNoProgress.isEmpty()) {
            throw new IllegalStateException("Members - " + membersWithNoProgress + "don't have progress");
        }

        var orphanProgress = peerProgress.keySet().stream()
                .filter(Predicate.not(members::contains))
                .collect(Collectors.toUnmodifiableSet());

        if (!orphanProgress.isEmpty()) {
            throw new IllegalStateException("Peer progress entries without config membership: " + orphanProgress);
        }

        var learnerProgressForNextLearners = mc.nextLearners()
                .stream()
                .filter(id -> peerProgress.get(id).isLearner())
                .collect(Collectors.toUnmodifiableSet());

        if (!learnerProgressForNextLearners.isEmpty()) {
            throw new IllegalStateException("Next Learners " + mc.nextLearners() + " are present in outgoing voters, but marked as " + RoleType.LEARNER);
        }

        var voterProgressForLearners = mc.learners()
                .stream()
                .filter(id -> peerProgress.get(id).isVoter())
                .collect(Collectors.toUnmodifiableSet());

        if (!voterProgressForLearners.isEmpty()) {
            throw new IllegalStateException("Learners " + mc.learners() + " are present in learners, but marked as " + RoleType.VOTER);
        }
    }

    public boolean canCheckQuorumAfterTick() {
        if (!checkQuorum)
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
        if ((uncommittedSize + size) > maxUncommittedSize)
            return false;

        uncommittedSize += size;
        return true;
    }

    public Function<NodeId, OptionalLong> matchIndexer() {
        return matchIndexer;
    }

    public boolean isLeaderTransferee(NodeId id) {
        return transferTarget.map(id::equals).orElse(false);
    }

    public boolean isLeaderTransferInProgress() {
        return transferTarget.isPresent();
    }


    public PeerProgress progress(NodeId id) {
        return peerProgress.get(id);
    }


    public Set<NodeId> peers() {
        return peers;
    }


    public Map<NodeId, Boolean> getQuorumVotesAndDeactivate() {
        var votes = new HashMap<NodeId, Boolean>(peerProgress.size());
        // Leader is always active — it wouldn't be running this check otherwise.
        votes.put(id, true);
        for (var peer: peers) {
            var progress = peerProgress.get(peer);
            votes.put(peer, progress.isActive());
            // Reset for next cycle: peer must respond again to be counted active.
            progress.setActive(false);
        }
        return votes;
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
        return readIndex.drainAcked(id, mc);
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



}
