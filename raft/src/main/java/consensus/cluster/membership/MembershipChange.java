package consensus.cluster.membership;

import consensus.core.NodeId;

public record MembershipChange(NodeId id, MembershipChangeType type) {
}

