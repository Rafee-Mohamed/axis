package consensus.cluster.progress;

/**
 * Defines the replication state machine for leader-to-follower communication.
 *
 * <p>A sealed interface with record implementations ensures type-safe state
 * transitions and eliminates invalid states at compile time.</p>
 *
 * <pre>
 * +------------------------------------------------------------+
 * |                  Valid State Transitions                   |
 * +------------------------------------------------------------+
 * |                                                            |
 * |      +----------- pause() -----------+                     |
 * |      |                               v                     |
 * |   +-------+                    +-------------+             |
 * |   | Probe |&lt;--- resume() ---| ProbePaused |             |
 * |   +-------+                    +-------------+             |
 * |      |                               |                     |
 * |      | toReplicate()                 | toReplicate()       |
 * |      v                               v                     |
 * |      +----------- pause() -----------+                     |
 * |      |                               v                     |
 * |   +-----------+              +-----------------+           |
 * |   | Replicate |&lt;- resume() -| ReplicatePaused |         |
 * |   +-----------+              +-----------------+           |
 * |      |                               |                     |
 * |      | toProbe()                     | toProbe()           |
 * |      |   (rejection)                 |   (rejection)       |
 * |      +---------------+---------------+                     |
 * |                      v                                     |
 * |                   +-------+                                |
 * |                   | Probe |                                |
 * |                   +-------+                                |
 * |                      ^                                     |
 * |                      | toProbe()                           |
 * |                      |                                     |
 * |                 +----------+                               |
 * |                 | Snapshot |&lt;-- toSnapshot(index)       |
 * |                 +----------+   (from any state except      |
 * |                                 Snapshot itself)           |
 * |                                                            |
 * +------------------------------------------------------------+
 * </pre>
 *
 * <p><b>States:</b></p>
 * <ul>
 *   <li><b>Probe</b> - finding match point with follower. Send one entry, wait for response.</li>
 *   <li><b>ProbePaused</b> - waiting for response before sending more.</li>
 *   <li><b>Replicate</b> - fast path. Pipeline many messages. Has {@link Inflight} for flow control.</li>
 *   <li><b>ReplicatePaused</b> - inflight buffer full. Resume when acknowledgments arrive.</li>
 *   <li><b>Snapshot</b> - follower too far behind. Sending snapshot. Always paused.</li>
 * </ul>
 *
 * <p><b>Invalid transitions</b> (enforced by design - methods don't exist):</p>
 * <ul>
 *   <li>Snapshot to Snapshot (no self-transition)</li>
 *   <li>Snapshot to Replicate (must probe first)</li>
 * </ul>
 *
 * <p>Transitions are modeled as instance methods on each record, so only
 * valid state changes compile. Each transition returns a new instance.</p>
 *
 * @see PeerProgress
 * @see Inflight
 */
public sealed interface ReplicationState {

    /**
     * Returns {@code true} if sending log entries is paused in this state.
     *
     * <p>Only {@link Probe} and {@link Replicate} are active sending states.</p>
     *
     * @return {@code true} if paused ({@link ProbePaused}, {@link ReplicatePaused}, or {@link Snapshot})
     */
    default boolean isPaused() {
        return !(this instanceof Probe || this instanceof Replicate);
    }

    /* ==================== PROBE STATES ==================== */

    /**
     * Probe state: finding the match point with the follower.
     *
     * <p>Sends one entry at a time and waits for response.
     * Initial state for new peers or after failures.</p>
     */
    record Probe() implements ReplicationState {

        /**
         * Pauses after sending an entry - wait for response before sending more.
         *
         * @return new ProbePaused state
         */
        public ProbePaused pause() { return new ProbePaused(); }

        /**
         * Transitions to Replicate when match point is found.
         *
         * @param inflight the flow control tracker for pipelining
         * @return new Replicate state
         */
        public Replicate toReplicate(Inflight inflight) {
            return new Replicate(inflight);
        }

        /**
         * Transitions to Snapshot when the follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /**
     * ProbePaused state: sent an entry in Probe, waiting for response.
     *
     * <p>No more entries sent until we hear back.</p>
     */
    record ProbePaused() implements ReplicationState {

        /**
         * Resumes to Probe on receiving a response from the follower.
         *
         * @return new Probe state
         */
        public Probe resume() { return new Probe(); }

        /**
         * Transitions to Replicate when match point is found.
         *
         * @param inflight the flow control tracker for pipelining
         * @return new Replicate state
         */
        public Replicate toReplicate(Inflight inflight) {
            return new Replicate(inflight);
        }

        /**
         * Transitions to Snapshot when the follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /* ==================== REPLICATE STATES ==================== */

    /**
     * Replicate state: fast path for replication.
     *
     * <p>Pipelines multiple messages optimistically.
     * Contains an {@link Inflight} tracker for flow control.</p>
     *
     * @param inflight the flow control tracker for this follower
     */
    record Replicate(Inflight inflight) implements ReplicationState {

        /**
         * Pauses when the inflight buffer is full.
         *
         * @return new ReplicatePaused state sharing the same inflight tracker
         */
        public ReplicatePaused pause() { return new ReplicatePaused(inflight); }

        /**
         * Transitions back to Probe on rejection (logs diverged).
         *
         * @return new Probe state
         */
        public Probe toProbe() { return new Probe(); }

        /**
         * Transitions to Snapshot when the follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /**
     * ReplicatePaused state: inflight buffer is full.
     *
     * <p>Waiting for acknowledgments before sending more.
     * Still contains {@link Inflight} to track pending messages.</p>
     *
     * @param inflight the flow control tracker (shared with {@link Replicate})
     */
    record ReplicatePaused(Inflight inflight) implements ReplicationState {

        /**
         * Resumes to Replicate when inflight space is freed by acknowledgments.
         *
         * @return new Replicate state sharing the same inflight tracker
         */
        public Replicate resume() { return new Replicate(inflight); }

        /**
         * Transitions back to Probe on rejection (logs diverged).
         *
         * @return new Probe state
         */
        public Probe toProbe() { return new Probe(); }

        /**
         * Transitions to Snapshot when the follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /* ==================== SNAPSHOT STATE ==================== */

    /**
     * Snapshot state: follower is too far behind.
     *
     * <p>Sending a snapshot to bring it up to speed.
     * Always paused (no log entries sent during snapshot).</p>
     *
     * @param index the index of the snapshot being sent (for progress tracking)
     */
    record Snapshot(long index) implements ReplicationState {

        /**
         * Transitions to Probe after the snapshot delivery completes.
         *
         * @return new Probe state
         */
        public Probe toProbe() { return new Probe(); }

    }
}
