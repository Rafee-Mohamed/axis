package consensus.algorithm;

import consensus.config.CandidateConfig;
import consensus.node.NodeId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.random.RandomGenerator;

public final class PreCandidate implements Role {
    private final Candidate candidate;
    public PreCandidate(CandidateConfig config, RandomGenerator random) {
        candidate = new Candidate(config, random);
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
