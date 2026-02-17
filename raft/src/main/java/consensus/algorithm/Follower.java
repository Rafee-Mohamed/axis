package consensus.algorithm;

import consensus.node.NodeId;

import java.util.Optional;

public final class Follower implements Role {
    private Optional<NodeId> leaderId;
    private int electionElapsed;
    private int electionTimeout;
    private int randomizedElectionTimeout;

    public Follower(NodeId leaderId) {
        this.leaderId = Optional.ofNullable(leaderId);
        electionElapsed = 0;
    }

    public void tick() {
        electionElapsed++;
    }

    public boolean canStartElectionAfterTick() {
        electionElapsed++;

        var canStartElection = electionElapsed >= randomizedElectionTimeout;

        if (canStartElection)
            electionElapsed = 0;

        return canStartElection;
    }

    public boolean hasLeader() {
        return leaderId.isPresent();
    }

    public void voteGranted() {
        leaderId = Optional.empty();
        electionElapsed = 0;
    }
}
