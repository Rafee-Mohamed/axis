package consensus.algorithm;

import consensus.node.NodeId;

import java.util.Optional;

public final class Learner implements Role {
    private Optional<NodeId> leader;
    private TickTimer leaseTimer;

    public Learner(NodeId leaderId) {
        leader = Optional.ofNullable(leaderId);
        leaseTimer = new TickTimer(10);
    }

    public boolean hasLeader() {
        return leader.isPresent();
    }

    public NodeId leaderId() {
        return leader.get();
    }

    public void setLeader(NodeId id) { leader = Optional.ofNullable(id); }
}
