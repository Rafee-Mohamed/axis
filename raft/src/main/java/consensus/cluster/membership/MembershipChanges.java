package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.HashSet;
import java.util.List;

/**
 * A batch of membership changes to apply atomically.
 *
 * <p>Validated at construction: a node may only appear once in the
 * change list. Duplicate node ids are rejected with an
 * {@link IllegalArgumentException}.</p>
 *
 * @param changes    the individual changes (one per node)
 * @param transition controls how the joint consensus phase is handled
 *                   ({@link MembershipTransition#AUTO},
 *                   {@link MembershipTransition#JOINT_AUTO}, or
 *                   {@link MembershipTransition#JOINT_EXPLICIT})
 */
public record MembershipChanges(List<MembershipChange> changes, MembershipTransition transition) {

    /**
     * Validates that no node id appears more than once.
     *
     * @throws IllegalArgumentException if duplicate node ids are found
     */
    public MembershipChanges {
        var seen = new HashSet<NodeId>();
        var duplicates = new HashSet<NodeId>();
        for (var c : changes) {
            if (!seen.add(c.id())) {
                duplicates.add(c.id());
            }
        }
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Node IDs " + duplicates + " appear more than once in membership changes. A single node can have only one change");
        }
    }
}
