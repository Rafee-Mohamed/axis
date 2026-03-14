package consensus.protocol.role;

/**
 * A monotonic tick counter checked against a timeout threshold.
 *
 * <p>Composed by different Raft roles to drive periodic operations such as
 * heartbeat broadcasts, quorum liveness checks, election timeouts, and
 * leader lease tracking. Encapsulates the tick-check-reset pattern so
 * timer logic stays in one place.</p>
 *
 * <p>The counter increments without bound (no capping) so callers
 * can query against multiple thresholds on the same counter via
 * {@link #isTimedOut(int)}.</p>
 */
public final class TickTimer {

    /** Number of ticks elapsed since the last reset. */
    private int elapsed;

    /** The primary timeout threshold (set at construction). */
    private final int timeout;

    /**
     * Creates a timer with the given primary timeout.
     *
     * @param timeout tick count at which the timer fires
     */
    public TickTimer(int timeout) {
        this.elapsed = 0;
        this.timeout = timeout;
    }

    /**
     * Advances the counter by one tick. Called once per Raft tick cycle.
     */
    public void tick() {
        elapsed++;
    }

    /**
     * Resets the counter to zero. Called when the timer fires
     * (automatic in {@link #resetIfTimedOutAfterTick()}) or when an
     * external event restarts the cycle.
     */
    public void reset() {
        elapsed = 0;
    }

    /**
     * Tick-check-reset in one call - the standard periodic timer pattern.
     *
     * <p>Increments the counter, checks against the primary timeout, and
     * resets if fired. Returns {@code true} exactly once per cycle when
     * the timeout elapses, then starts a fresh cycle.</p>
     *
     * @return {@code true} if the timer fired this tick
     */
    public boolean resetIfTimedOutAfterTick() {
        tick();
        if (isTimedOut()) {
            reset();
            return true;
        }
        return false;
    }

    /**
     * Checks whether the counter has reached the primary timeout.
     *
     * @return {@code true} if elapsed ticks {@code >=} the primary timeout
     */
    public boolean isTimedOut() {
        return elapsed >= timeout;
    }

    /**
     * Checks whether the counter has reached a custom threshold.
     * Used when a single timer serves multiple purposes at different
     * thresholds.
     *
     * @param threshold the tick count to check against
     * @return {@code true} if elapsed ticks {@code >=} {@code threshold}
     */
    public boolean isTimedOut(int threshold) {
        return elapsed >= threshold;
    }
}
