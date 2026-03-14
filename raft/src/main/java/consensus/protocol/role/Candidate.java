package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.CandidateConfig;

import java.util.*;
import java.util.function.Function;
import java.util.random.RandomGenerator;

/**
 * A voting member running a real election to become leader.
 *
 * <p>Under {@link consensus.protocol.policy.ElectionProtocol#DUAL_ELECTION},
 * the node transitions here after winning a pre-election as
 * {@link PreCandidate}. Under
 * {@link consensus.protocol.policy.ElectionProtocol#DIRECT_ELECTION},
 * the node transitions directly from {@link Follower}. In both cases
 * the candidate increments its term, votes for itself, and broadcasts
 * {@link consensus.message.Message.RequestVote} messages. Incoming
 * responses are recorded in the {@code votes} map and evaluated via
 * quorum checks.</p>
 *
 * <p>The election round timer is randomized at construction. If it
 * fires before a quorum is reached, the candidate restarts the
 * election in a new term.</p>
 *
 * <p>Also used internally by {@link PreCandidate} via composition -
 * the vote-tracking and timer logic is identical for both phases.</p>
 */
public final class Candidate implements Role {

    /**
     * Votes received so far: peer id to granted ({@code true}) or
     * rejected ({@code false}). Peers not yet responded are absent.
     */
    private final Map<NodeId, Boolean> votes;

    /**
     * Lookup function over {@link #votes}. Returns {@code Optional.empty()}
     * for peers that have not yet responded, enabling quorum calculation
     * to distinguish "no response" from "rejected."
     */
    private final Function<NodeId, Optional<Boolean>> voteQuery;

    /**
     * Election round timer with a randomized timeout in the range
     * {@code [electionRoundTimeout, 2 * electionRoundTimeout)}. If it
     * fires before a quorum is reached, the election round restarts.
     */
    private final TickTimer electionRoundTimer;

    /**
     * Creates a candidate with an empty vote tally and a randomized
     * election round timer.
     *
     * @param config candidate configuration (election round timeout)
     * @param random source of randomness for timeout jitter
     */
    public Candidate(CandidateConfig config, RandomGenerator random) {
        votes = new HashMap<>();
        voteQuery = id -> Optional.ofNullable(votes.getOrDefault(id, null));
        electionRoundTimer = new TickTimer(config.electionRoundTimeout() + random.nextInt(config.electionRoundTimeout()));
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link RoleType#CANDIDATE}
     */
    @Override
    public RoleType type() {
        return RoleType.CANDIDATE;
    }

    /**
     * Records a vote from a peer. Duplicate votes from the same peer
     * are ignored.
     *
     * @param voter   the peer that voted
     * @param granted {@code true} if the vote was granted, {@code false} if rejected
     * @return {@code true} if the vote was newly recorded,
     *         {@code false} if this peer already voted
     */
    public boolean recordVote(NodeId voter, boolean granted) {
        if (votes.containsKey(voter)) {
            return false;
        }
        votes.put(voter, granted);
        return true;
    }

    /**
     * Ticks the election round timer and returns {@code true} if it
     * fired, indicating the round timed out without reaching a quorum.
     * The timer auto-resets on fire.
     *
     * @return {@code true} if the election round timer expired this tick
     */
    public boolean electionRoundTimedOutAfterTick() {
        return electionRoundTimer.resetIfTimedOutAfterTick();
    }

    /**
     * Returns a function that looks up a peer's vote result. Returns
     * {@code Optional.empty()} for peers that have not yet responded.
     *
     * @return the vote lookup function for quorum evaluation
     */
    public Function<NodeId, Optional<Boolean>> voteQuery() {
        return voteQuery;
    }
}
