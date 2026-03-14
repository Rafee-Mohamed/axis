package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.CandidateConfig;

import java.util.Optional;
import java.util.function.Function;
import java.util.random.RandomGenerator;

/**
 * A voting member running a pre-election phase before starting a real election.
 *
 * <p>Only used when
 * {@link consensus.protocol.policy.ElectionProtocol#DUAL_ELECTION} is
 * configured. The pre-election prevents disruption from partitioned
 * nodes: the node sends {@link consensus.message.Message.RequestPreVote}
 * messages at the <em>next</em> term without incrementing its own term.
 * Only if a majority would grant the vote does the node proceed to
 * {@link Candidate} with a real term increment.</p>
 *
 * <p>Delegates vote tracking and timer logic to an internal
 * {@link Candidate} instance - the mechanics are identical, only the
 * message type and term semantics differ.</p>
 */
public final class PreCandidate implements Role {

    /** Internal delegate handling vote tracking and the election round timer. */
    private final Candidate candidate;

    /**
     * Creates a pre-candidate backed by an internal {@link Candidate}.
     *
     * @param config candidate configuration (election round timeout)
     * @param random source of randomness for timeout jitter
     */
    public PreCandidate(CandidateConfig config, RandomGenerator random) {
        candidate = new Candidate(config, random);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link RoleType#PRE_CANDIDATE}
     */
    @Override
    public RoleType type() {
        return RoleType.PRE_CANDIDATE;
    }

    /**
     * Delegates to {@link Candidate#recordVote(NodeId, boolean)}.
     *
     * @param voter   the peer that voted
     * @param granted {@code true} if the pre-election vote was granted
     * @return {@code true} if the vote was newly recorded
     */
    public boolean recordVote(NodeId voter, boolean granted) {
       return candidate.recordVote(voter, granted);
    }

    /**
     * Delegates to {@link Candidate#electionRoundTimedOutAfterTick()}.
     *
     * @return {@code true} if the election round timer expired this tick
     */
    public boolean electionRoundTimedOutAfterTick() {
        return candidate.electionRoundTimedOutAfterTick();
    }

    /**
     * Delegates to {@link Candidate#voteQuery()}.
     *
     * @return the vote lookup function for quorum evaluation
     */
    public Function<NodeId, Optional<Boolean>> voteQuery() {
        return candidate.voteQuery();
    }
}
