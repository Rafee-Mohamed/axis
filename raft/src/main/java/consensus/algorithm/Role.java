package consensus.algorithm;

import consensus.membership.MembershipConfig;
import consensus.node.NodeId;

import java.util.Map;

public sealed interface Role  {
    record Leader(Map<NodeId, Boolean> votes, MembershipConfig config, Map<NodeId, PeerProgress> peerProgress) implements Role{};
    record Candidate(Map<NodeId, Boolean> votes) implements Role{};
    record Follower() implements Role{};
    record Learner() implements Role{};
}

