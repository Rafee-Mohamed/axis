package consensus.membership;

import consensus.algorithm.NodeId;

public record MembershipChange(NodeId id, MembershipChangeType type) {
}

