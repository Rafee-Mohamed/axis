package consensus.membership;

import consensus.algorithm.Leader;
import consensus.node.NodeId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public record MembershipConfig(
        JointConfig voters,
        Set<NodeId> learners,
        Set<NodeId> nextLearners,
        MembershipTransition transition
) {

    public static MembershipConfig empty() {
        return new MembershipConfig(
                new JointConfig(new MajorityConfig(Set.of()), null),
                Set.of(),
                Set.of(),
                MembershipTransition.JOINT_AUTO
        );
    }

    public static MembershipConfig simple(Set<NodeId> voters) {
        return new MembershipConfig(
                new JointConfig(new MajorityConfig(voters), null),
                Set.of(),
                Set.of(),
                MembershipTransition.JOINT_AUTO
        );
    }

    /**
     * Check if in joint consensus.
     */
    public boolean isJoint() {
        return voters.isJoint();
    }

    /**
     * Committed index (delegates to JointConfig).
     */
    public long committedIndex(MatchIndexer indexer) {
        return voters.committedIndex(indexer);
    }

    /**
     * Vote result (delegates to JointConfig).
     */
    public VoteResult voteResult(Map<NodeId, Boolean> votes) {
        return voters.voteResult(votes);
    }

    /**
     * All nodes that should receive replication (voters + learners).
     */
    public Set<NodeId> allReplicationTargets() {
        var targets = new HashSet<>(voters.allVoters());
        targets.addAll(learners);
        return targets;
    }

    /**
     * Checks if a node is a member of the cluster — either a voter (in any
     * config during joint consensus) or a learner.
     *
     * <p>Does NOT check {@code nextLearners}. By invariant, any node in
     * nextLearners is guaranteed to also be in the outgoing voter set
     * (the {@code current} config during joint consensus), so
     * {@code voters.contains(id)} will find it.</p>
     *
     * @param id the node to check
     * @return true if the node is a voter or learner in this configuration
     */
    public boolean isMember(NodeId id) {
        return voters.contains(id) || learners.contains(id);
    }

    public boolean isLearner(NodeId transferee) {
        return learners.contains(transferee);
    }
}
