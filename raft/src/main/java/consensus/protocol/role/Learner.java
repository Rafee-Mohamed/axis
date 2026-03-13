package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.LearnerConfig;

import java.util.Optional;

public final class Learner implements Replicant {
    private Optional<NodeId> leader;
    private final TickTimer leaseTimer;

    public Learner(NodeId leaderId, LearnerConfig config) {
        this.leader = Optional.ofNullable(leaderId);
        this.leaseTimer = new TickTimer(config.leaseTimeout());
    }

    public Learner(LearnerConfig config) {
        this(null, config);
    }

    public RoleType type() {
        return RoleType.LEARNER;
    }

    public boolean hasLeader() {
        return leader.isPresent();
    }

    public NodeId leaderId() {
        return leader.get();
    }

    public void setLeader(NodeId id) {
        leader = Optional.ofNullable(id);
        if (leader.isPresent()) {
            renewLease();
        }
    }

    /**
     * Clears the known leader, making this node a leaderless learner.
     * The node remains in its current term.
     */
    public void forgetLeader() {
        leader = Optional.empty();
    }

    public boolean leaseExpiredAfterTick() {
        return leaseTimer.resetIfTimedOutAfterTick();
    }

    public void renewLease() {
        leaseTimer.reset();
    }
}
