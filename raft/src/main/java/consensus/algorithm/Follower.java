package consensus.algorithm;

import consensus.node.NodeId;

import java.util.Optional;

public final class Follower implements Role {
    private Optional<NodeId> leader;
    private final int electionTimeout; // base (non-randomized) timeout for lease checks
    private final TickTimer electionTimer;

    public Follower(NodeId leaderId) {
        this.leader = Optional.ofNullable(leaderId);
        this.electionTimer = new TickTimer(10);
        this.electionTimeout = 0;
    }


    public void resetElectionTimer() {
       electionTimer.reset();
    }

    public boolean canStartElectionAfterTick() {
        return electionTimer.resetIfTimedOutAfterTick();
    }

    public boolean isElectionTimedOut() {
        return electionTimer.isTimedOut(electionTimeout);
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
}
