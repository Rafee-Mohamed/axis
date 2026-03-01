package consensus.membership;

import consensus.node.NodeId;

import java.util.HashSet;
import java.util.List;

public record MembershipChanges(List<MembershipChange> changes, MembershipTransition transition) {
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
