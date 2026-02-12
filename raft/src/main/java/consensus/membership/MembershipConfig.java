package consensus.membership;

import consensus.node.NodeId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public record MembershipConfig(
        JointConfig voters,
        Set<NodeId> learners,
        Set<NodeId> nextLearners,
        boolean autoLeave
) {

    public static MembershipConfig empty() {
        return new MembershipConfig(
                new JointConfig(new MajorityConfig(Set.of()), null),
                Set.of(),
                Set.of(),
                false
        );
    }

    public static MembershipConfig simple(Set<NodeId> voters) {
        return new MembershipConfig(
                new JointConfig(new MajorityConfig(voters), null),
                Set.of(),
                Set.of(),
                false
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
    public long committedIndex(Map<NodeId, Long> matchIndices) {
        return voters.committedIndex(matchIndices);
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
}
