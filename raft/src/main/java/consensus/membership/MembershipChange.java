package consensus.membership;

import consensus.node.NodeId;

import java.util.Objects;

public record MembershipChange(NodeId id, MembershipChangeType type) {
}

