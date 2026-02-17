package consensus.algorithm;

import consensus.node.NodeId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class Candidate implements Role {

    private final Map<NodeId, Boolean> votes;

    public Candidate() {
        this.votes = new HashMap<>();
    }

    public boolean recordVote(NodeId voter, boolean granted) {
        if (votes.containsKey(voter)) {
            return false;
        }

        votes.put(voter, granted);
        return true;
    }

    public void tallyVotes() {

    }

    public Map<NodeId, Boolean> getVotes() {
        return votes;
    }
}
