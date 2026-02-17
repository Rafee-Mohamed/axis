package consensus.algorithm;

/**
 * Tracks replication progress from leader to a single follower.
 * 
 * The leader maintains one PeerProgress instance per peer (including itself)
 * to track how far each follower's log has been replicated and to decide what
 * entries to send next.
 * 
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────────────────────┐
 * │                    PeerProgress: Leader's View of a Follower                    │
 * ├─────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                 │
 * │  Leader's Log:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]                         │
 * │                              ↑     ↑         ↑                                  │
 * │                           match  sentCommit  next                               │
 * │                            =5       =7       =10                                │
 * │                                                                                 │
 * │  Ranges:                                                                        │
 * │  ─────────────────────────────────────────────────────────────────────────────  │
 * │  [1, match]         = Confirmed replicated (follower acknowledged)              │
 * │  (match, next)      = In-flight (sent, awaiting acknowledgment)                 │
 * │  [next, lastIndex]  = Not yet sent                                              │
 * │                                                                                 │
 * │  sentCommit         = Highest commit index we've told follower about            │
 * │                       (avoids redundant commit updates)                         │
 * │                                                                                 │
 * │  Invariants:                                                                    │
 * │  ─────────────────────────────────────────────────────────────────────────────  │
 * │  • match < next                    (always, next is at least match + 1)         │
 * │  • sentCommit <= next - 1          (can't commit what we haven't sent)          │
 * │  • next >= 1                       (next is always at least 1)                  │
 * │                                                                                 │
 * └─────────────────────────────────────────────────────────────────────────────────┘
 * 
 * ┌─────────────────────────────────────────────────────────────────────────────────┐
 * │                              State Transitions                                  │
 * ├─────────────────────────────────────────────────────────────────────────────────┤
 * │                                                                                 │
 * │       ┌─────────┐  found match   ┌───────────┐  rejection   ┌─────────┐         │
 * │       │  PROBE  │ ─────────────► │ REPLICATE │ ───────────► │  PROBE  │         │
 * │       └─────────┘                └───────────┘              └─────────┘         │
 * │            │                          │                                         │
 * │            │ needs snapshot           │ needs snapshot                          │
 * │            ▼                          ▼                                         │
 * │       ┌──────────────────────────────────────┐                                  │
 * │       │              SNAPSHOT                 │                                 │
 * │       │  (waiting for snapshot to complete)   │ ──── done ────► PROBE           │
 * │       └──────────────────────────────────────┘                                  │
 * │                                                                                 │
 * │  PROBE:     Finding match point. Send one entry, wait for response.             │
 * │  REPLICATE: Fast path. Send many entries optimistically.                        │
 * │  SNAPSHOT:  Follower too far behind. Send snapshot, pause entries.              │
 * │                                                                                 │
 * └─────────────────────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * @see ReplicationState
 * @see Inflight
 */
public class PeerProgress {
    
    /**
     * Match is the index up to which the follower's log is known to match the
     * leader's. The leader is certain the follower has entries [1, match].
     * Updated when we receive a successful AppendEntriesResponse.
     */
    private long match;
    
    /**
     * Next is the log index of the next entry to send to this follower.
     * Entries in the (match, next) interval are already in flight.
     *
     * Invariant: 0 <= match < next
     * Note: It follows that next >= 1
     */
    private long next;
    
    /**
     * sentCommit is the highest commit index in flight to the follower.
     * Used to avoid sending redundant commit updates in heartbeats.
     *
     * Generally monotonic, but can regress when converting to Probe state
     * or when receiving a rejection (the sent commit may no longer be valid).
     *
     * Note: sentCommit can be > match when commit is sent with in-flight entries.
     * Invariant: sentCommit <= next - 1
     */
    private long sentCommit;
    
    /** State defines how the leader should interact with this follower. */
    private ReplicationState state;

    /**
     * Role of this peer in the cluster: VOTER or LEARNER.
     * Learners receive log entries but don't participate in elections.
     */
    private RoleType roleType;
    
    /**
     * Active is true if the progress is recently active. Receiving any
     * message from the corresponding follower indicates the progress is active.
     * Can be reset to false after an election timeout.
     * Used for liveness detection and leader transfer decisions.
     */
    private boolean active;
    
    /** Configuration for creating Inflight instances when entering Replicate state. */
    private final Inflight.Config inflightConfig;
    
    /**
     * Creates a new PeerProgress.
     *
     * @param inflightConfig configuration for flow control (max messages, max bytes)
     */
    public PeerProgress(Inflight.Config inflightConfig) {
        this.inflightConfig = inflightConfig;
        this.roleType = RoleType.FOLLOWER;
        this.state = new ReplicationState.Probe();
        this.active = false;
        this.match = 0;
        this.sentCommit = 0;
        this.next = 1;
    }

    /* ==================== GETTERS ==================== */
    
    /** Returns the highest confirmed replicated index. */
    public long match() { return match; }
    
    /** Returns the next index to send. */
    public long next() { return next; }
    
    /** Returns the last commit index sent to follower. */
    public long sentCommit() { return sentCommit; }
    
    /** Returns whether follower has responded recently. */
    public boolean isActive() { return active; }
    
    /** Sets the active status. */
    public void setActive(boolean active) { this.active = active; }
    
    /** Returns true if this peer is a learner (non-voting member). */
    public boolean isLearner() { return roleType == RoleType.LEARNER; }
    
    /** Marks this peer as a learner. */
    public void becomeLearner() { roleType = RoleType.LEARNER; }

    /**
     * Returns whether sending log entries to this follower is paused.
     * This happens when:
     * - In Probe and waiting for response (ProbePaused)
     * - In Replicate and inflights are full (ReplicatePaused)
     * - In Snapshot (always paused)
     *
     * @return true if sending is paused
     */
    public boolean isPaused() {
        return state.isPaused();
    }

    /* ==================== STATE TRANSITIONS ==================== */

    /**
     * Transitions into Probe state. Next is reset to match+1 or,
     * if coming from Snapshot state and larger, the snapshot index + 1.
     *
     * Called when:
     * - Rejection received (logs diverged, need to find new match point)
     * - Snapshot completed (resume with probing)
     *
     * Example (from Replicate):
     *   Before: match=5, next=10, sentCommit=8
     *   After:  match=5, next=6,  sentCommit=5
     *
     * Example (from Snapshot with index=20):
     *   Before: match=5, next=21
     *   After:  match=5, next=21 (max of 21 and 6)
     */
    public void becomeProbe() {
        if (state instanceof  ReplicationState.Snapshot(long snapshotIndex)) {
            next = Math.max(snapshotIndex + 1, match + 1);
        } else {
            next = match + 1;
        }

        state = switch (state){
            case ReplicationState.Probe p -> p;
            case ReplicationState.ProbePaused pp -> pp.resume();
            case ReplicationState.Replicate r -> r.toProbe();
            case ReplicationState.ReplicatePaused rp -> rp.toProbe();
            case ReplicationState.Snapshot s -> s.toProbe();
        };

        sentCommit = Math.min(sentCommit, next - 1);
    }

    /**
     * Transitions into Replicate state, resetting next to match+1.
     * Creates a new Inflight tracker for flow control.
     *
     * Called when match point is found and we're ready for fast replication.
     * Cannot transition directly from Snapshot (must go through Probe first).
     *
     * Example:
     *   Before: match=10, next=11, state=Probe
     *   After:  match=10, next=11, state=Replicate(new Inflight)
     *
     * @throws IllegalStateException if called from Snapshot state
     */
    public void becomeReplicate() {
        state  = switch (state) {
            case ReplicationState.Probe p -> p.toReplicate(new Inflight(inflightConfig));
            case ReplicationState.ProbePaused pp -> pp.toReplicate(new Inflight(inflightConfig));
            case ReplicationState.Replicate r -> r;
            case ReplicationState.ReplicatePaused rp -> rp.resume();
            default -> throw new IllegalStateException("Cannot go from state " + state + "  to Replicate");
        };

        next = match + 1;
    }

    /**
     * Moves the progress to Snapshot state with the specified snapshot index.
     *
     * Called when follower needs entries that have been compacted from leader's log.
     * While in Snapshot state, replication is paused until snapshot completes.
     *
     * Example (snapshotIndex=100):
     *   Before: match=5,   next=10,  sentCommit=8
     *   After:  match=5,   next=101, sentCommit=100
     *
     * @param snapshotIndex the index of the snapshot being sent
     * @throws IllegalStateException if already in Snapshot state
     */
    public void becomeSnapshot(long snapshotIndex) {
        state = switch (state) {
            case ReplicationState.Probe p -> p.toSnapshot(snapshotIndex);
            case ReplicationState.ProbePaused pp -> pp.toSnapshot(snapshotIndex);
            case ReplicationState.Replicate r -> r.toSnapshot(snapshotIndex);
            case ReplicationState.ReplicatePaused rp -> rp.toSnapshot(snapshotIndex);
            default -> throw new IllegalStateException("Cannot go from state " + state + "  to Snapshot");
        };
        
        next = snapshotIndex + 1;
        sentCommit = snapshotIndex;
    }

    /* ==================== SENDING ==================== */

    /**
     * Updates progress after sending AppendEntries with the given
     * number of entries and total byte size.
     *
     * In Replicate state: optimistically advances next and tracks in inflights.
     * In Probe state: pauses after sending (wait for response).
     *
     * Must be called with Probe or Replicate state (not Snapshot).
     *
     * Example (Replicate, sending 3 entries of 150 bytes):
     *   Before: match=5, next=6
     *   After:  match=5, next=9, inflights=[(8, 150)]
     *
     * Example (Probe, sending 1 entry):
     *   Before: state=Probe
     *   After:  state=ProbePaused
     *
     * @param entries number of entries sent in the message
     * @param bytes   total byte size of entries sent
     * @throws IllegalStateException if called in Snapshot state
     */
    public void sentEntries(int entries, long bytes) {
        state = switch (state) {
            case ReplicationState.Replicate r -> {
                if (entries > 0) {
                    next += entries;
                    r.inflight().add(next - 1, bytes);
                }
                yield r.inflight().isFull() ? r.pause() : r;
            }
            case ReplicationState.Probe p -> entries > 0 ? p.pause() : p;
            default ->
                throw new IllegalStateException("Cannot sent entries in " + state + " state");
        };
    }

    /**
     * Returns true if sending the given commit index can potentially
     * advance the follower's commit index.
     *
     * Returns true only if:
     * - The commit is higher than what we've sent (index > sentCommit)
     * - We haven't already sent a commit covering all sent entries (sentCommit < next - 1)
     *
     * Example:
     *   sentCommit=5, next=10
     *   canAdvanceCommit(7)  → true  (7 > 5 and 5 < 9)
     *   canAdvanceCommit(5)  → false (5 > 5 is false)
     *   canAdvanceCommit(12) → true  (12 > 5 and 5 < 9)
     *
     * @param index the commit index we want to send
     * @return true if sending this commit would be useful
     */
    public boolean canAdvanceCommit(long index) {
        return index > sentCommit && sentCommit < (next - 1);
    }

    /**
     * Updates the sentCommit to track that we've communicated
     * this commit index to the follower.
     *
     * @param commitIndex the commit index that was sent
     */
    public void sentCommit(long commitIndex) {
        sentCommit = commitIndex;
    }

    /* ==================== RECEIVING RESPONSES ==================== */

    /**
     * Resumes sending if currently in a paused state.
     * ProbePaused -> Probe, ReplicatePaused -> Replicate.
     */
    public void resumeStateIfPaused() {
        state = switch (state) {
            case ReplicationState.ReplicatePaused rp -> rp.resume();
            case ReplicationState.ProbePaused pp -> pp.resume();
            default -> state;
        };
    }

    /**
     * Called when an AppendEntries was accepted. Updates match and frees
     * acknowledged messages from inflights.
     *
     * Returns false if index doesn't advance match (stale response).
     *
     * Example (successful update):
     *   Before: match=5, next=10, inflights=[(6,10), (7,20), (8,30)]
     *   tryUpdate(7) → true
     *   After:  match=7, next=10, inflights=[(8,30)]
     *
     * Example (stale response):
     *   Before: match=5
     *   tryUpdate(4) → false
     *   After:  match=5 (unchanged)
     *
     * @param index the index acknowledged by the follower
     * @return true if match was advanced, false if stale response
     */
    public boolean tryUpdate(long index) {
        if (index <= match) {
            return false;
        }

        match = index;
        next = Math.max(next, match + 1);

        switch (state) {
            case ReplicationState.Replicate r -> r.inflight().removeMessagesUpto(match);
            case ReplicationState.ReplicatePaused rp -> rp.inflight().removeMessagesUpto(match);
            default ->  {}
        }

        resumeStateIfPaused();

        return true;
    }

    /**
     * Called when an AppendEntries was rejected. Decrements next to find
     * a matching point.
     *
     * In Replicate state:
     * - Only relevant if rejectedIndex > match (otherwise stale)
     * - Resets next to match+1 and transitions to Probe
     *
     * In Probe state:
     * - Only relevant if rejectedIndex == next-1 (the exact entry we sent)
     * - Uses lastMatchedIndex hint from follower to optimize next decrement
     * - Does not decrement below match+1
     *
     * Example (Probe state, follower hints at lastMatchedIndex=3):
     *   Before: match=0, next=10
     *   tryDecrementTo(9, 3) → true
     *   After:  match=0, next=4 (min(9, 4) = 4, max(4, 1) = 4)
     *
     * Example (stale rejection):
     *   Before: match=5, next=10
     *   tryDecrementTo(6, 3) → false (6 != 9)
     *   After:  unchanged
     *
     * @param rejectedIndex    the index that was rejected by the follower
     * @param lastMatchedIndex the hint from follower about its last matched index
     * @return true if next was decremented, false if stale rejection
     */
    public boolean tryDecrementTo(long rejectedIndex, long lastMatchedIndex) {
        if (state instanceof ReplicationState.Replicate || state instanceof ReplicationState.ReplicatePaused) {
            if (rejectedIndex <= match) {
                return false;
            }

            next = match + 1;
            sentCommit = Math.min(sentCommit, next - 1);
            return true;
        }

        if (next - 1 != rejectedIndex) {
            return false;
        }

        next = Math.max(Math.min(rejectedIndex, lastMatchedIndex + 1), match + 1);
        sentCommit = Math.min(sentCommit, next - 1);
        resumeStateIfPaused();
        return true;
    }

}
