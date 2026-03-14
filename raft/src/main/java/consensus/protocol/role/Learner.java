package consensus.protocol.role;

import consensus.core.NodeId;
import consensus.config.LearnerConfig;

import java.util.Optional;

/**
 * A non-voting member that replicates entries from the current leader.
 *
 * <p>A learner receives the same AppendEntries and snapshot traffic as
 * a follower but never participates in elections or votes. It cannot
 * become a candidate. Learners are used for read replicas, catch-up
 * nodes being promoted, or observer nodes.</p>
 *
 * <p>Instead of an election timer, a learner runs a lease timer. If no
 * communication is received from the leader before the lease expires,
 * the learner clears its leader reference (it cannot campaign, but it
 * can stop serving stale reads that depend on leader liveness).</p>
 */
public final class Learner implements Replicant {

    /** Current leader, empty when unknown. */
    private Optional<NodeId> leader;

    /**
     * Lease timer tracking liveness of the leader. Renewed on every
     * leader contact; when it fires the learner drops its leader
     * reference since it can no longer confirm leader liveness.
     */
    private final TickTimer leaseTimer;

    /**
     * Creates a learner with an optional known leader.
     *
     * @param leaderId the current leader's id, or {@code null} if unknown
     * @param config   learner configuration (lease timeout, etc.)
     */
    public Learner(NodeId leaderId, LearnerConfig config) {
        this.leader = Optional.ofNullable(leaderId);
        this.leaseTimer = new TickTimer(config.leaseTimeout());
    }

    /**
     * Creates a learner with no known leader.
     *
     * @param config learner configuration
     */
    public Learner(LearnerConfig config) {
        this(null, config);
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link RoleType#LEARNER}
     */
    @Override
    public RoleType type() {
        return RoleType.LEARNER;
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
     *
     * @throws java.util.NoSuchElementException if no leader is known
     */
    @Override
    public NodeId leaderId() {
        return leader.get();
    }

    /**
     * Sets or clears the leader reference. If a leader is now known,
     * the lease timer is renewed - the learner just heard from it.
     *
     * @param id the leader's node id, or {@code null} to clear
     */
    @Override
    public void setLeader(NodeId id) {
        leader = Optional.ofNullable(id);
        if (leader.isPresent()) {
            renewLease();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forgetLeader() {
        leader = Optional.empty();
    }

    /**
     * Ticks the lease timer and returns {@code true} if it expired,
     * indicating loss of contact with the leader. The timer
     * auto-resets on expiry.
     *
     * @return {@code true} if the lease expired this tick
     */
    public boolean leaseExpiredAfterTick() {
        return leaseTimer.resetIfTimedOutAfterTick();
    }

    /**
     * Resets the lease timer to zero. Called when the learner receives
     * valid communication from the leader.
     */
    public void renewLease() {
        leaseTimer.reset();
    }
}
