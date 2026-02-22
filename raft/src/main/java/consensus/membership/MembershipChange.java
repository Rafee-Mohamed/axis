package consensus.membership;

import consensus.node.NodeId;

enum MembershipChangeType {
    REMOVE,
    ADD_VOTER,
    ADD_LEARNER,
    // other types to add
}

public record MembershipChange(NodeId id, MembershipChangeType type) {}

