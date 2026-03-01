package consensus.algorithm;

import consensus.config.FollowerConfig;
import consensus.node.NodeId;

import java.util.Optional;

public final class Follower implements Role {
    private Optional<NodeId> leader;
    private final FollowerConfig config;
    private final TickTimer electionTimer;

    public Follower(NodeId leaderId, FollowerConfig config) {
        this.leader = Optional.ofNullable(leaderId);
        this.config = config;
        this.electionTimer = new TickTimer(config.electionTimeout());
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
