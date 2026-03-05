package consensus.algorithm;

import consensus.config.CandidateConfig;
import consensus.node.NodeId;

import java.util.*;
import java.util.function.Function;
import java.util.random.RandomGenerator;

public final class Candidate implements Role {

    private final Map<NodeId, Boolean> votes;
    private final Function<NodeId, Optional<Boolean>> voteQuery;
    private final TickTimer electionRoundTimer;

    public Candidate(CandidateConfig config, RandomGenerator random) {
        votes = new HashMap<>();
        voteQuery = id -> Optional.ofNullable(votes.getOrDefault(id, null));
        electionRoundTimer = new TickTimer(config.electionRoundTimeout() + random.nextInt(config.electionRoundTimeout()));
    }

    public boolean recordVote(NodeId voter, boolean granted) {
        if (votes.containsKey(voter)) {
            return false;
        }
        votes.put(voter, granted);
        return true;
    }

    public boolean electionRoundTimedOutAfterTick() {
        return electionRoundTimer.resetIfTimedOutAfterTick();
    }

    public Function<NodeId, Optional<Boolean>> voteQuery() {
        return voteQuery;
    }
}
