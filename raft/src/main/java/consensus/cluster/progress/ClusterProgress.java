package consensus.cluster.progress;

import consensus.core.NodeId;
import consensus.cluster.membership.MemberType;
import consensus.cluster.membership.MembershipConfig;
import consensus.config.PeerInflightConfig;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages replication progress for all peers in the cluster.
 *
 * <p>Created by the leader upon election, this class holds a
 * {@link PeerProgress} for every member (voters and learners alike,
 * including the leader itself). It is the single source of truth for
 * deciding what to send to each follower, whether commits can advance,
 * and which peers are alive.</p>
 *
 * <p>The progress map is kept in sync with the cluster's
 * {@link MembershipConfig} through {@link #applyNewMembership} -
 * called after every membership change is applied.</p>
 *
 * @see PeerProgress
 * @see MembershipConfig
 */
public class ClusterProgress {

    /** Per-peer replication progress, keyed by node id. */
    private Map<NodeId, PeerProgress> progress;

    /** Broadcast set: all tracked peers excluding the leader itself. */
    private Set<NodeId> peers;

    /** The node id of the leader that owns this progress tracker. */
    private final NodeId leaderId;

    /** Configuration used to create new {@link Inflight} instances. */
    private final PeerInflightConfig inflightConfig;

    /**
     * Creates a new cluster progress tracker for the given membership.
     *
     * <p>Initializes a {@link PeerProgress} for every member. The
     * leader's own progress is immediately set to
     * {@link ReplicationState.Replicate Replicate} with
     * {@code match = lastIndex}.</p>
     *
     * @param mc             the current membership configuration
     * @param inflightConfig flow control limits for each peer
     * @param leaderId       the id of the leader creating this tracker
     * @param lastIndex      the leader's last log index
     * @throws IllegalStateException if the leader is not in the membership config
     */
    public ClusterProgress(
            MembershipConfig mc,
            PeerInflightConfig inflightConfig,
            NodeId leaderId,
            long lastIndex
    ) {
        this.leaderId = leaderId;
        this.inflightConfig = inflightConfig;
        constructPeerProgress(mc, lastIndex);
    }

    /**
     * Returns the id of the leader that owns this progress tracker.
     *
     * @return the leader's node id
     */
    public NodeId leaderId() {
        return leaderId;
    }

    /**
     * Returns the full progress map (all members including the leader).
     *
     * @return unmodifiable view is not guaranteed - callers should treat as read-only
     */
    public Map<NodeId, PeerProgress> progress() {
        return progress;
    }

    /**
     * Builds the initial progress map from the membership config.
     *
     * <p>Every member gets a fresh {@link PeerProgress} starting in
     * {@link ReplicationState.Probe Probe} state. The leader's own
     * progress is then promoted to
     * {@link ReplicationState.Replicate Replicate}.</p>
     *
     * @param mc        the membership configuration
     * @param lastIndex the leader's last log index used as match for the leader
     * @throws IllegalStateException if the leader is not in the config
     */
    private void constructPeerProgress(MembershipConfig mc, long lastIndex) {
        var members = mc.members();
        if (!members.contains(leaderId)) {
            throw new IllegalStateException("MembershipConfig don't have the leader");
        }

        var newPeerProgress = new HashMap<NodeId, PeerProgress>(members.size());

        for (var member: members) {
            var newProgress = new PeerProgress(inflightConfig, mc.memberType(member), lastIndex);
            newPeerProgress.put(member, newProgress);
        }

        var leaderProgress = newPeerProgress.get(leaderId);
        leaderProgress.becomeReplicate();
        leaderProgress.setActive(true);

        progress = newPeerProgress;
        validateInvariants(mc);
        constructPeers();
    }

    /**
     * Reconciles the leader's peer progress map with a new membership config.
     *
     * <p>Called by the Raft layer after every membership change is applied to
     * the state machine - on both enter-joint and leave-joint transitions.
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
     *   <li><b>Existing node, promoted (learner to voter)</b>: the role type is
     *       upgraded. The node now participates in commit and election quorums.</li>
     *   <li><b>Existing node, demoted (voter to learner)</b>: the role type is
     *       downgraded. The node no longer counts toward quorum. Note: during
     *       joint consensus, a node in nextLearners is still a voter in the
     *       outgoing config - demotion only takes effect at leave-joint.</li>
     * </ul>
     *
     * <p>Nodes that were in the old config but are absent from the new config
     * are dropped - they don't appear in the new progress map. This handles
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
            var existing = progress.get(member);
            if (existing == null) {
                var newProgress = new PeerProgress(inflightConfig, mc.memberType(member), lastIndex);
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
        progress = newPeerProgress;
        validateInvariants(mc);
        constructPeers();
    }

    /**
     * Returns the leader's own progress entry.
     *
     * @return the leader's {@link PeerProgress}, never {@code null}
     */
    public PeerProgress leaderProgress() {
        return progress.get(leaderId);
    }

    /**
     * Rebuilds the broadcast peer set (all tracked peers excluding the leader).
     */
    private void constructPeers() {
        var newPeers = new HashSet<>(progress.keySet());
        newPeers.remove(leaderId);
        peers = Collections.unmodifiableSet(newPeers);
    }

    /**
     * Returns a function that maps a node id to its match index.
     *
     * <p>Used by quorum commit calculation to determine the highest
     * index replicated to a majority. Returns {@link OptionalLong#empty()}
     * for unknown nodes.</p>
     *
     * @return a mapper from node id to optional match index
     */
    public Function<NodeId, OptionalLong> matchIndexer() {
        return id -> {
            var peerProgress = progress.get(id);
            return peerProgress == null ? OptionalLong.empty() : OptionalLong.of(peerProgress.match());
        };
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
     *       VOTER progress - they must not count toward commit quorum.</li>
     * </ol>
     *
     * @param mc the membership config to validate against
     * @throws IllegalStateException if any invariant is violated
     */
    private void validateInvariants(MembershipConfig mc) {
        var members = mc.members();
        var membersWithNoProgress = members.stream()
                .filter(Predicate.not(progress::containsKey))
                .collect(Collectors.toUnmodifiableSet());

        if (!membersWithNoProgress.isEmpty()) {
            throw new IllegalStateException("Members - " + membersWithNoProgress + "don't have progress");
        }

        var orphanProgress = progress.keySet().stream()
                .filter(Predicate.not(members::contains))
                .collect(Collectors.toUnmodifiableSet());

        if (!orphanProgress.isEmpty()) {
            throw new IllegalStateException("Peer progress entries without config membership: " + orphanProgress);
        }

        var learnerProgressForNextLearners = mc.nextLearners()
                .stream()
                .filter(id -> progress.get(id).isLearner())
                .collect(Collectors.toUnmodifiableSet());

        if (!learnerProgressForNextLearners.isEmpty()) {
            throw new IllegalStateException("Next Learners " + mc.nextLearners() + " are present in outgoing voters, but marked as " + MemberType.LEARNER);
        }

        var voterProgressForLearners = mc.learners()
                .stream()
                .filter(id -> progress.get(id).isVoter())
                .collect(Collectors.toUnmodifiableSet());

        if (!voterProgressForLearners.isEmpty()) {
            throw new IllegalStateException("Learners " + mc.learners() + " are present in learners, but marked as " + MemberType.VOTER);
        }
    }

    /**
     * Returns the progress for a specific peer.
     *
     * @param id the node id to look up
     * @return the peer's progress, or {@code null} if not tracked
     */
    public PeerProgress progress(NodeId id) {
        return progress.get(id);
    }

    /**
     * Returns {@code true} if a progress entry exists for the given node.
     *
     * @param id the node id to check
     * @return {@code true} if the node is tracked
     */
    public boolean hasProgress(NodeId id) {
        return progress.containsKey(id);
    }

    /**
     * Returns the broadcast peer set (all tracked peers excluding the leader).
     *
     * @return an unmodifiable set of peer node ids
     */
    public Set<NodeId> peers() {
        return peers;
    }

    /**
     * Collects liveness votes from all voters and resets their active flags.
     *
     * <p>Used by the leader liveness check
     * ({@code LeaderLivenessPolicy.QUORUM_VERIFIED}). The leader votes
     * for itself (always active). Each voter peer's current active status
     * is captured, then all peers are marked inactive for the next cycle -
     * they must respond again to be counted active.</p>
     *
     * <p>Learners are not included in the vote map since they do not
     * participate in quorum decisions, but their active flags are still
     * reset.</p>
     *
     * @return a map of voter node id to active status
     */
    public Map<NodeId, Boolean> getQuorumVotesAndDeactivate() {
        var votes = new HashMap<NodeId, Boolean>(progress.size());
        votes.put(leaderId, true);
        for (var peer: peers) {
            var peerProgress = progress.get(peer);
            if (peerProgress.isVoter()) {
                votes.put(peer, peerProgress.isActive());
            }
            peerProgress.setActive(false);
        }
        return votes;
    }
}
