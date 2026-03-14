package consensus.cluster.progress;

import consensus.cluster.membership.MemberType;
import consensus.config.PeerInflightConfig;

/**
 * Tracks replication progress from leader to a single follower.
 *
 * <p>The leader maintains one {@code PeerProgress} instance per peer
 * (including itself) to track how far each follower's log has been
 * replicated and to decide what entries to send next.</p>
 *
 * <pre>
 * +-------------------------------------------------------------------+
 * |            PeerProgress: Leader's View of a Follower              |
 * +-------------------------------------------------------------------+
 * |                                                                   |
 * |  Leader's Log:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]           |
 * |                              ^     ^         ^                    |
 * |                           match  sentCommit  next                 |
 * |                            =5       =7       =10                  |
 * |                                                                   |
 * |  Ranges:                                                          |
 * |  ---------------------------------------------------------        |
 * |  [1, match]         = confirmed replicated (acknowledged)         |
 * |  (match, next)      = in-flight (sent, awaiting ack)              |
 * |  [next, lastIndex]  = not yet sent                                |
 * |                                                                   |
 * |  sentCommit         = highest commit index told to follower       |
 * |                       (avoids redundant commit updates)           |
 * |                                                                   |
 * |  Invariants:                                                      |
 * |  ---------------------------------------------------------        |
 * |  match  &lt; next             (next is at least match + 1)        |
 * |  sentCommit &lt;= next - 1   (can't commit unsent entries)        |
 * |  next &gt;= 1                                                     |
 * |                                                                   |
 * +-------------------------------------------------------------------+
 *
 * +-------------------------------------------------------------------+
 * |                      State Transitions                            |
 * +-------------------------------------------------------------------+
 * |                                                                   |
 * |  +-------+  found match  +-----------+  rejection  +-------+      |
 * |  | PROBE | -----------> | REPLICATE | ----------> | PROBE |       |
 * |  +-------+              +-----------+             +-------+       |
 * |      |                       |                                    |
 * |      | needs snapshot        | needs snapshot                     |
 * |      v                       v                                    |
 * |  +------------------------------------+                           |
 * |  |             SNAPSHOT               |                           |
 * |  | (waiting for snapshot to complete) |--- done ---> PROBE        |
 * |  +------------------------------------+                           |
 * |                                                                   |
 * +-------------------------------------------------------------------+
 * </pre>
 *
 * @see ReplicationState
 * @see Inflight
 */
public class PeerProgress {
    
    /**
     * The index up to which the follower's log is known to match the leader's.
     *
     * <p>The leader is certain the follower has entries [1, match].
     * Updated when we receive a successful AppendEntriesResponse.</p>
     */
    private long match;

    /**
     * The log index of the next entry to send to this follower.
     *
     * <p>Entries in the (match, next) interval are already in flight.</p>
     *
     * <p>Invariant: {@code 0 <= match < next} (it follows that {@code next >= 1}).</p>
     */
    private long next;

    /**
     * The highest commit index in flight to the follower.
     *
     * <p>Used to avoid sending redundant commit updates in heartbeats.</p>
     *
     * <p>Generally monotonic, but can regress when converting to Probe state
     * or when receiving a rejection (the sent commit may no longer be valid).</p>
     *
     * <p>Note: sentCommit can be &gt; match when commit is sent with in-flight
     * entries. Invariant: {@code sentCommit <= next - 1}.</p>
     */
    private long sentCommit;
    
    /** State defines how the leader should interact with this follower. */
    private ReplicationState state;

    /**
     * Membership type of this peer: {@link MemberType#VOTER} or {@link MemberType#LEARNER}.
     *
     * <p>Learners receive log entries but don't participate in elections.</p>
     */
    private MemberType memberType;

    /**
     * Whether this peer has responded recently.
     *
     * <p>Receiving any message from the corresponding follower indicates the
     * progress is active. Reset to {@code false} after an election timeout.
     * Used for liveness detection and leader transfer decisions.</p>
     */
    private boolean active;
    
    /** Configuration for creating Inflight instances when entering Replicate state. */
    private final PeerInflightConfig inflightConfig;
    
    /**
     * Creates a new {@code PeerProgress} with match index 0.
     *
     * @param inflightConfig configuration for flow control (max messages, max bytes)
     * @param memberType     membership type of this peer ({@link MemberType#VOTER} or {@link MemberType#LEARNER})
     */
    public PeerProgress(PeerInflightConfig inflightConfig, MemberType memberType) {
        this(inflightConfig, memberType, 0);
    }

    /**
     * Creates a new {@code PeerProgress} with a given match index.
     *
     * <p>Starts in {@link ReplicationState.Probe Probe} state with
     * {@code next = matchIndex + 1} and {@code sentCommit = 0}.</p>
     *
     * @param inflightConfig configuration for flow control (max messages, max bytes)
     * @param memberType     membership type of this peer ({@link MemberType#VOTER} or {@link MemberType#LEARNER})
     * @param matchIndex     the highest log index known to be replicated
     */
    public PeerProgress(PeerInflightConfig inflightConfig, MemberType memberType, long matchIndex) {
        this.inflightConfig = inflightConfig;
        this.memberType = memberType;
        this.state = new ReplicationState.Probe();
        this.active = true;
        this.match = matchIndex;
        this.sentCommit = 0;
        this.next = matchIndex + 1;
    }

    /* ==================== GETTERS ==================== */

    /**
     * Returns the highest confirmed replicated index.
     *
     * @return the match index
     */
    public long match() { return match; }

    /**
     * Returns the next log index to send to this follower.
     *
     * @return the next index
     */
    public long next() { return next; }

    /**
     * Returns the current replication state of this peer.
     *
     * @return the replication state, never {@code null}
     */
    public ReplicationState state() { return state; }

    /**
     * Returns the highest commit index already sent to this follower.
     *
     * @return the sent commit index
     */
    public long sentCommit() { return sentCommit; }

    /**
     * Returns whether this follower has responded recently.
     *
     * @return {@code true} if active
     */
    public boolean isActive() { return active; }

    /**
     * Sets the active status of this peer.
     *
     * @param active {@code true} to mark as active, {@code false} to mark inactive
     */
    public void setActive(boolean active) { this.active = active; }

    /**
     * Returns {@code true} if this peer is a learner (non-voting member).
     *
     * @return {@code true} if learner
     */
    public boolean isLearner() { return memberType == MemberType.LEARNER; }

    /**
     * Returns {@code true} if this peer is a voter.
     *
     * @return {@code true} if voter
     */
    public boolean isVoter() { return memberType == MemberType.VOTER; }

    /**
     * Demotes this peer to learner. The peer will no longer count
     * toward commit or election quorums.
     */
    public void becomeLearner() { memberType = MemberType.LEARNER; }

    /**
     * Promotes this peer to voter. The peer will now count
     * toward commit and election quorums.
     */
    public void becomeVoter() { memberType = MemberType.VOTER; }

    /**
     * Returns whether sending log entries to this follower is paused.
     *
     * <p>Sending is paused when:</p>
     * <ul>
     *   <li>In Probe and waiting for response (ProbePaused)</li>
     *   <li>In Replicate and inflights are full (ReplicatePaused)</li>
     *   <li>In Snapshot (always paused)</li>
     * </ul>
     *
     * @return {@code true} if sending is paused
     */
    public boolean isPaused() {
        return state.isPaused();
    }

    /**
     * Returns {@code true} if the peer is in a probing state
     * ({@link ReplicationState.Probe} or {@link ReplicationState.ProbePaused}).
     *
     * @return {@code true} if probing
     */
    public boolean probing() {
        return state instanceof ReplicationState.Probe || state instanceof ReplicationState.ProbePaused;
    }

    /**
     * Returns {@code true} if the peer is in a replicating state
     * ({@link ReplicationState.Replicate} or {@link ReplicationState.ReplicatePaused}).
     *
     * @return {@code true} if replicating
     */
    public boolean replicating() {
        return state instanceof ReplicationState.Replicate || state instanceof ReplicationState.ReplicatePaused;
    }

    /* ==================== STATE TRANSITIONS ==================== */

    /**
     * Transitions into Probe state.
     *
     * <p>Next is reset to {@code match + 1} or, if coming from Snapshot state
     * and larger, {@code snapshotIndex + 1}.</p>
     *
     * <p>Called when:</p>
     * <ul>
     *   <li>Rejection received (logs diverged, need to find new match point)</li>
     *   <li>Snapshot completed (resume with probing)</li>
     * </ul>
     *
     * <pre>
     * Example (from Replicate):
     *   Before: match=5, next=10, sentCommit=8
     *   After:  match=5, next=6,  sentCommit=5
     *
     * Example (from Snapshot with index=20):
     *   Before: match=5, next=21
     *   After:  match=5, next=21 (max of 21 and 6)
     * </pre>
     */
    public void becomeProbe() {
        if (state instanceof  ReplicationState.Snapshot(long snapshotIndex)) {
            next = Math.max(snapshotIndex + 1, match + 1);
        } else {
            next = match + 1;
        }

        state = switch (state) {
            case ReplicationState.Probe p -> p;
            case ReplicationState.ProbePaused pp -> pp.resume();
            case ReplicationState.Replicate r -> r.toProbe();
            case ReplicationState.ReplicatePaused rp -> rp.toProbe();
            case ReplicationState.Snapshot s -> s.toProbe();
        };

        sentCommit = Math.min(sentCommit, next - 1);
    }

    /**
     * Transitions into Replicate state, resetting next to {@code match + 1}.
     *
     * <p>Creates a new {@link Inflight} tracker for flow control. Called when
     * match point is found and we're ready for fast replication. Cannot
     * transition directly from Snapshot (must go through Probe first).</p>
     *
     * <pre>
     * Example:
     *   Before: match=10, next=11, state=Probe
     *   After:  match=10, next=11, state=Replicate(new Inflight)
     * </pre>
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
     * <p>Called when follower needs entries that have been compacted from the
     * leader's log. While in Snapshot state, replication is paused until the
     * snapshot completes.</p>
     *
     * <pre>
     * Example (snapshotIndex=100):
     *   Before: match=5,   next=10,  sentCommit=8
     *   After:  match=5,   next=101, sentCommit=100
     * </pre>
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

    /**
     * Handles a failed snapshot delivery by resetting progress to Probe.
     *
     * <p>Unlike {@link #becomeProbe()} called from Snapshot state (which
     * probes from {@code snapshotIndex + 1} because the snapshot was applied),
     * this method probes from {@code match + 1} - the snapshot was never
     * applied, so we go back to the last confirmed match point.</p>
     *
     * <p>Example (snapshot at index 100, match at 5):</p>
     * <pre>
     *   Before: state=Snapshot(100), match=5, next=101
     *   After:  state=Probe,         match=5, next=6
     *
     *   vs becomeProbe() from Snapshot (success):
     *   After:  state=Probe,         match=5, next=101
     * </pre>
     *
     * @throws IllegalStateException if not in Snapshot state
     */
    public void snapshotFailed() {
        if (state instanceof ReplicationState.Snapshot s) {
            next = match + 1;
            sentCommit = Math.min(sentCommit, next - 1);
            state = s.toProbe();
            return;
        }
        throw new IllegalStateException("Snapshot failure can happen only in ReplicationState.Snapshot but happened in " + state);
    }

    /* ==================== SENDING ==================== */

    /**
     * Updates progress after sending AppendEntries with the given
     * number of entries and total byte size.
     *
     * <ul>
     *   <li><b>Replicate</b> - optimistically advances next and tracks in inflights.</li>
     *   <li><b>Probe</b> - pauses after sending (wait for response).</li>
     * </ul>
     *
     * <p>Must be called with Probe or Replicate state (not Snapshot).</p>
     *
     * <pre>
     * Example (Replicate, sending 3 entries of 150 bytes):
     *   Before: match=5, next=6
     *   After:  match=5, next=9, inflights=[(8, 150)]
     *
     * Example (Probe, sending 1 entry):
     *   Before: state=Probe
     *   After:  state=ProbePaused
     * </pre>
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
     * Returns {@code true} if sending the given commit index can potentially
     * advance the follower's commit index.
     *
     * <p>Returns {@code true} only if:</p>
     * <ul>
     *   <li>The commit is higher than what we've sent ({@code index > sentCommit})</li>
     *   <li>We haven't already sent a commit covering all sent entries
     *       ({@code sentCommit < next - 1})</li>
     * </ul>
     *
     * <pre>
     * Example:
     *   sentCommit=5, next=10
     *   canAdvanceCommit(7)  -> true  (7 &gt; 5 and 5 &lt; 9)
     *   canAdvanceCommit(5)  -> false (5 &gt; 5 is false)
     *   canAdvanceCommit(12) -> true  (12 &gt; 5 and 5 &lt; 9)
     * </pre>
     *
     * @param index the commit index we want to send
     * @return {@code true} if sending this commit would be useful
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
     *
     * <p>ProbePaused to Probe, ReplicatePaused to Replicate.
     * No-op if already resumed or in Snapshot state.</p>
     */
    public void resumeStateIfPaused() {
        state = switch (state) {
            case ReplicationState.ReplicatePaused rp -> rp.resume();
            case ReplicationState.ProbePaused pp -> pp.resume();
            default -> state;
        };
    }

    /**
     * Pauses sending if currently in an active (unpaused) state.
     *
     * <p>Probe to ProbePaused, Replicate to ReplicatePaused.
     * Already-paused and Snapshot states are unaffected.</p>
     *
     * <p>Used after snapshot status handling to prevent the leader from
     * immediately sending AppendEntries - on success, we wait for the
     * peer's AppendEntriesResponse; on failure, we wait for the next
     * heartbeat cycle before retrying.</p>
     *
     * @see #resumeStateIfPaused()
     */
    public void pauseStateIfResumed() {
        state = switch (state) {
            case ReplicationState.Replicate r -> r.pause();
            case ReplicationState.Probe p -> p.pause();
            default -> state;
        };
    }

    /**
     * Called when an AppendEntries was accepted. Updates match and frees
     * acknowledged messages from inflights.
     *
     * <p>Returns {@code false} if the index doesn't advance match (stale response).</p>
     *
     * <pre>
     * Example (successful update):
     *   Before: match=5, next=10, inflights=[(6,10), (7,20), (8,30)]
     *   tryUpdate(7) -> true
     *   After:  match=7, next=10, inflights=[(8,30)]
     *
     * Example (stale response):
     *   Before: match=5
     *   tryUpdate(4) -> false
     *   After:  match=5 (unchanged)
     * </pre>
     *
     * @param index the index acknowledged by the follower
     * @return {@code true} if match was advanced, {@code false} if stale response
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
     * <p><b>In Replicate state:</b></p>
     * <ul>
     *   <li>Only relevant if {@code rejectedIndex > match} (otherwise stale)</li>
     *   <li>Resets next to {@code match + 1} and transitions to Probe</li>
     * </ul>
     *
     * <p><b>In Probe state:</b></p>
     * <ul>
     *   <li>Only relevant if {@code rejectedIndex == next - 1} (the exact entry we sent)</li>
     *   <li>Uses {@code matchIndexHint} from follower to optimize next decrement</li>
     *   <li>Does not decrement below {@code match + 1}</li>
     * </ul>
     *
     * <pre>
     * Example (Probe state, follower hints at lastMatchedIndex=3):
     *   Before: match=0, next=10
     *   tryDecrementTo(9, 3) -> true
     *   After:  match=0, next=4 (min(9, 4) = 4, max(4, 1) = 4)
     *
     * Example (stale rejection):
     *   Before: match=5, next=10
     *   tryDecrementTo(6, 3) -> false (6 != 9)
     *   After:  unchanged
     * </pre>
     *
     * @param rejectedIndex  the index that was rejected by the follower
     * @param matchIndexHint hint from follower about its last matched index
     * @return {@code true} if next was decremented, {@code false} if stale rejection
     */
    public boolean tryDecrementTo(long rejectedIndex, long matchIndexHint) {
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

        next = Math.max(Math.min(rejectedIndex, matchIndexHint + 1), match + 1);
        sentCommit = Math.min(sentCommit, next - 1);
        resumeStateIfPaused();
        return true;
    }

    /**
     * Returns {@code true} if the leader can send more entries to this peer.
     *
     * <p>Sending is allowed when in Probe (one message at a time) or
     * Replicate with available inflight capacity. All other states
     * (paused or snapshot) return {@code false}.</p>
     *
     * @return {@code true} if more messages can be sent
     */
    public boolean canReplicateMessages() {
        return switch (state) {
            case ReplicationState.Replicate(var inflight)-> !inflight.isFull();
            case ReplicationState.Probe p -> true;
            default -> false;
        };
    }
}
