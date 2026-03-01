package consensus.algorithm;

import consensus.node.NodeId;

import java.util.*;
import java.util.function.Function;

public final class Candidate implements Role {

    private final Map<NodeId, Boolean> votes;
    private final Function<NodeId, Optional<Boolean>> voteQuery;

    public Candidate() {
        votes = new HashMap<>();
        voteQuery = id -> Optional.ofNullable(votes.getOrDefault(id, null));
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

    public Function<NodeId, Optional<Boolean>> getVotes() {
        return voteQuery;
    }
}
