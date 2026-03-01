package consensus.algorithm;

import consensus.node.NodeId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class PreCandidate implements Role {
    private final Candidate candidate;
    public PreCandidate() {
        candidate = new Candidate();
    }

    public boolean recordVote(NodeId voter, boolean granted) {
       return candidate.recordVote(voter, granted);
    }

    public void tallyVotes() {

    }

    public Function<NodeId, Optional<Boolean>> voteQuery() {
        return candidate.voteQuery();
    }
}
