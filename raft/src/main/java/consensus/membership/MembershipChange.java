package consensus.membership;

import consensus.node.NodeId;

public sealed interface MembershipChange {
    record AddVoter(NodeId nodeId) implements MembershipChange {}
    record RemoveVoter(NodeId nodeId) implements MembershipChange {}
    record AddLearner(NodeId nodeId) implements MembershipChange {}
    record PromoteLearner(NodeId nodeId) implements MembershipChange {}  // learner → voter
    record DemoteVoter(NodeId nodeId) implements MembershipChange {}     // voter → learner (rare)
}

