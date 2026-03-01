package consensus.algorithm;

import consensus.config.CandidateConfig;
import consensus.node.NodeId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class PreCandidate implements Role {
    private final Candidate candidate;
    public PreCandidate(CandidateConfig config) {
        candidate = new Candidate(config);
    }

    public boolean recordVote(NodeId voter, boolean granted) {
       return candidate.recordVote(voter, granted);
    }

    public boolean electionRoundTimedOutAfterTick() {
        return candidate.electionRoundTimedOutAfterTick();
    }

    public Function<NodeId, Optional<Boolean>> voteQuery() {
        return candidate.voteQuery();
    }
}
