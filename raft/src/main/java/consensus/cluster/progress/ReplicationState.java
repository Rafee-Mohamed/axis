package consensus.cluster.progress;

/**
 * Defines the replication state machine for leader-to-follower communication.
 * 
 * A sealed interface with record implementations ensures type-safe state
 * transitions and eliminates invalid states at compile time.
 * 
 * <pre>
 * ┌──────────────────────────────────────────────────────────┐
 * │               Valid State Transitions                    │
 * ├──────────────────────────────────────────────────────────┤
 * │                                                          │
 * │      ┌─────────── pause() ───────────┐                   │
 * │      │                               ▼                   │
 * │   ┌──────┐                    ┌─────────────┐            │
 * │   │Probe │◄── resume() ────── │ ProbePaused │            │
 * │   └──────┘                    └─────────────┘            │
 * │      │                               │                   │
 * │      │ toReplicate()                 │ toReplicate()     │
 * │      ▼                               ▼                   │
 * │      ┌─────────── pause() ───────────┐                   │
 * │      │                               ▼                   │
 * │   ┌───────────┐              ┌─────────────────┐         │
 * │   │ Replicate │◄─ resume() ──│ ReplicatePaused │         │
 * │   └───────────┘              └─────────────────┘         │
 * │      │                               │                   │
 * │      │ toProbe()                     │ toProbe()         │
 * │      │   (rejection)                 │   (rejection)     │
 * │      └───────────────┬───────────────┘                   │
 * │                      ▼                                   │
 * │                   ┌──────┐                               │
 * │                   │Probe │                               │
 * │                   └──────┘                               │
 * │                      ▲                                   │
 * │                      │ toProbe()                         │
 * │                      │                                   │
 * │                 ┌──────────┐                             │
 * │                 │ Snapshot │◄── toSnapshot(index)        │
 * │                 └──────────┘    (from ANY state except   │
 * │                                  Snapshot itself)        │
 * │                                                          │
 * └──────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * States:
 * - Probe: Finding match point with follower. Send one entry, wait for response.
 * - ProbePaused: Waiting for response before sending more.
 * - Replicate: Fast path. Pipeline many messages. Has Inflight for flow control.
 * - ReplicatePaused: Inflight buffer full. Resume when acknowledgments arrive.
 * - Snapshot: Follower too far behind. Sending snapshot. Always paused.
 * 
 * Invalid transitions (enforced by design - methods don't exist):
 * - Snapshot → Snapshot (no self-transition)
 * - Snapshot → Replicate (must probe first)
 * 
 * Design rationale:
 * - Sealed interface: Only these 5 states exist, enforced at compile time.
 * - Records: Immutable states. Transitions create new instances.
 * - Instance methods for transitions: Ensures only valid transitions compile.
 * - Inflight in Replicate states: Flow control only needed during fast replication.
 * - Index in Snapshot: Tracks pending snapshot for progress reporting.
 * 
 * @see PeerProgress
 * @see Inflight
 */
public sealed interface ReplicationState {

    /**
     * Returns true if sending log entries is paused in this state.
     * Only Probe and Replicate are active sending states.
     *
     * @return true if paused (ProbePaused, ReplicatePaused, or Snapshot)
     */
    default boolean isPaused() {
        return !(this instanceof Probe || this instanceof Replicate);
    }

    /* ==================== PROBE STATES ==================== */

    /**
     * Probe state: Finding the match point with the follower.
     * Sends one entry at a time and waits for response.
     * Initial state for new peers or after failures.
     */
    record Probe() implements ReplicationState {
        
        /** Pause after sending an entry. */
        public ProbePaused pause() { return new ProbePaused(); }
        
        /**
         * Transition to Replicate when match is found.
         *
         * @param inflight the flow control tracker
         * @return new Replicate state
         */
        public Replicate toReplicate(Inflight inflight) {
            return new Replicate(inflight);
        }
        
        /**
         * Transition to Snapshot when follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /**
     * ProbePaused state: Sent an entry in Probe, waiting for response.
     * No more entries sent until we hear back.
     */
    record ProbePaused() implements ReplicationState {
        
        /** Resume to Probe on receiving response. */
        public Probe resume() { return new Probe(); }
        
        /**
         * Transition to Replicate when match is found.
         *
         * @param inflight the flow control tracker
         * @return new Replicate state
         */
        public Replicate toReplicate(Inflight inflight) {
            return new Replicate(inflight);
        }
        
        /**
         * Transition to Snapshot when follower needs a snapshot.
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
     * Replicate state: Fast path for replication.
     * Pipelines multiple messages optimistically.
     * Contains an Inflight tracker for flow control.
     *
     * @param inflight the flow control tracker for this follower
     */
    record Replicate(Inflight inflight) implements ReplicationState {
        
        /** Pause when inflight is full. */
        public ReplicatePaused pause() { return new ReplicatePaused(inflight); }
        
        /** Transition to Probe on rejection (logs diverged). */
        public Probe toProbe() { return new Probe(); }
        
        /**
         * Transition to Snapshot when follower needs a snapshot.
         *
         * @param pendingSnapshotIndex the index of the snapshot being sent
         * @return new Snapshot state
         */
        Snapshot toSnapshot(long pendingSnapshotIndex) {
            return new Snapshot(pendingSnapshotIndex);
        }
    }

    /**
     * ReplicatePaused state: Inflight buffer is full.
     * Waiting for acknowledgments before sending more.
     * Still contains Inflight to track pending messages.
     *
     * @param inflight the flow control tracker (shared with Replicate)
     */
    record ReplicatePaused(Inflight inflight) implements ReplicationState {
        
        /** Resume to Replicate when space is freed. */
        public Replicate resume() { return new Replicate(inflight); }
        
        /** Transition to Probe on rejection. */
        public Probe toProbe() { return new Probe(); }
        
        /**
         * Transition to Snapshot when follower needs a snapshot.
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
     * Snapshot state: Follower is too far behind.
     * Sending a snapshot to bring it up to speed.
     * Always paused (no log entries sent during snapshot).
     *
     * @param index the index of the snapshot being sent (for progress tracking)
     */
    record Snapshot(long index) implements ReplicationState {
        
        /** Transition to Probe after snapshot completes. */
        public Probe toProbe() { return new Probe(); }
        
        /* Note: No toSnapshot - cannot transition from Snapshot to Snapshot. */
    }
}
