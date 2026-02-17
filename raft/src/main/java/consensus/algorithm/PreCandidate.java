package consensus.algorithm;

import consensus.node.NodeId;

import java.util.HashMap;
import java.util.Map;

public final class PreCandidate implements Role {
    private final Map<NodeId, Boolean> votes;

    public PreCandidate() {
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
