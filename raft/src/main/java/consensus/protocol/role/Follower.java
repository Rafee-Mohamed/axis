package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.FollowerConfig;

import java.util.Optional;
import java.util.random.RandomGenerator;

/**
 * A voting member that replicates entries from the current leader.
 *
 * <p>A follower tracks the current leader, runs an election timer, and
 * forwards client proposals and reads to the leader. If the election
 * timer fires without hearing from a leader, the node transitions to
 * {@link PreCandidate} under
 * {@link consensus.protocol.policy.ElectionProtocol#DUAL_ELECTION}, or
 * directly to {@link Candidate} under
 * {@link consensus.protocol.policy.ElectionProtocol#DIRECT_ELECTION}.</p>
 *
 * <p>The election timeout is randomized at construction to reduce
 * split-vote probability across the cluster.</p>
 */
public final class Follower implements Replicant {

    /** Current leader, empty when unknown (e.g. after startup or leader failure). */
    private Optional<NodeId> leader;

    /** Follower-specific configuration (election timeout, etc.). */
    private final FollowerConfig config;

    /**
     * Election timer with a randomized timeout in the range
     * {@code [electionTimeout, 2 * electionTimeout)}. Reset on every
     * leader contact; fires to trigger a campaign.
     */
    private final TickTimer electionTimer;

    /**
     * Creates a follower with an optional known leader.
     *
     * @param leaderId the current leader's id, or {@code null} if unknown
     * @param config   follower configuration
     * @param random   source of randomness for election timeout jitter
     */
    public Follower(NodeId leaderId, FollowerConfig config, RandomGenerator random) {
        this.leader = Optional.ofNullable(leaderId);
        this.config = config;
        this.electionTimer = new TickTimer(config.electionTimeout() + random.nextInt(config.electionTimeout()));
    }

    /**
     * Creates a follower with no known leader.
     *
     * @param config follower configuration
     * @param random source of randomness for election timeout jitter
     */
    public Follower(FollowerConfig config, RandomGenerator random) {
        this(null, config, random);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link RoleType#FOLLOWER}
     */
    @Override
    public RoleType type() {
        return RoleType.FOLLOWER;
    }

    /**
     * Resets the election timer to zero. Called when the follower
     * receives valid communication from the leader (AppendEntries,
     * heartbeat, or snapshot).
     */
    public void resetElectionTimer() {
       electionTimer.reset();
    }

    /**
     * Ticks the election timer and returns {@code true} if it fired,
     * indicating the follower should start an election campaign. The
     * timer auto-resets on fire.
     *
     * @return {@code true} if the election timer expired this tick
     */
    public boolean canStartElectionAfterTick() {
        return electionTimer.resetIfTimedOutAfterTick();
    }

    /**
     * Checks whether the election timeout has elapsed without resetting.
     * Used by leadership transfer to verify the transferee has not been
     * unresponsive for too long.
     *
     * @return {@code true} if elapsed ticks exceed the base election timeout
     */
    public boolean isElectionTimedOut() {
        return electionTimer.isTimedOut(config.electionTimeout());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasLeader() {
        return leader.isPresent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLeader(NodeId id) {
        leader = Optional.ofNullable(id);
    }

    /**
     * Records that this follower granted its vote to a candidate.
     * Clears the leader reference (the old leader is no longer
     * recognized) and resets the election timer so the voter does
     * not immediately campaign itself.
     */
    public void voteGranted() {
        leader = Optional.empty();
        electionTimer.reset();
    }

    /**
     * {@inheritDoc}
     *
     * @throws java.util.NoSuchElementException if no leader is known
     */
    @Override
    public NodeId leaderId() {
        return leader.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forgetLeader() {
        leader = Optional.empty();
    }
}
