package consensus.algorithm;

import consensus.node.NodeId;

import java.util.Optional;

public final class Learner implements Role {
    private Optional<NodeId> leaderId;

    public boolean hasLeader() {
        return leaderId.isPresent();
    }
}
