package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.FollowerConfig;

import java.util.Optional;
import java.util.random.RandomGenerator;

public final class Follower implements Replicant {
    private Optional<NodeId> leader;
    private final FollowerConfig config;
    private final TickTimer electionTimer;

    public Follower(NodeId leaderId, FollowerConfig config, RandomGenerator random) {
        this.leader = Optional.ofNullable(leaderId);
        this.config = config;
        this.electionTimer = new TickTimer(config.electionTimeout() + random.nextInt(config.electionTimeout()));
    }

    public RoleType type() {
        return RoleType.FOLLOWER;
    }

    public Follower(FollowerConfig config, RandomGenerator random) {
        this(null, config, random);
    }


    public void resetElectionTimer() {
       electionTimer.reset();
    }

    public boolean canStartElectionAfterTick() {
        return electionTimer.resetIfTimedOutAfterTick();
    }

    public boolean isElectionTimedOut() {
        return electionTimer.isTimedOut(config.electionTimeout());
    }

    public boolean hasLeader() {
        return leader.isPresent();
    }

    public void setLeader(NodeId id) {
        leader = Optional.ofNullable(id);
    }

    public void voteGranted() {
        leader = Optional.empty();
        electionTimer.reset();
    }

    public NodeId leaderId() {
        return leader.get();
    }

    /**
     * Clears the known leader, making this node a leaderless follower.
     * The node remains in its current term and does not campaign.
     */
    public void forgetLeader() {
        leader = Optional.empty();
    }
}
