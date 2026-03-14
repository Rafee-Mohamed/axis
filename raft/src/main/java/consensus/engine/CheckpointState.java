package consensus.engine;

/**
 * The committed index checkpoint - persisted as a performance optimization.
 *
 * <p>Not required for safety. On recovery, a node can rediscover the
 * commit point from the leader. Persisting it avoids re-applying
 * already-committed entries after a restart.</p>
 *
 * @param commit the highest committed log index
 */
public record CheckpointState(long commit) {

    /**
     * Returns {@code true} if the commit index differs from the given value.
     *
     * @param commit the commit index to compare against
     * @return {@code true} if the commit index has changed
     */
    public boolean hasChanged(long commit) {
        return this.commit != commit;
    }
}
