package consensus.node;

import consensus.algorithm.ReadState;
import consensus.engine.VolatileState;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks {@link CompletableFuture}s for linearizable read requests,
 * reconciling them against the Raft core's read confirmation and
 * release signals.
 *
 * <h2>Read Lifecycle in Raft</h2>
 * <p>A linearizable read goes through three stages inside the Raft
 * core before the application can safely serve it:</p>
 * <ol>
 *   <li><b>Registered</b> -- the read request is submitted; the leader
 *       records its current commit index and initiates a heartbeat
 *       round to confirm authority.</li>
 *   <li><b>Confirmed</b> -- a heartbeat majority ack proves the leader
 *       is still authoritative. The read enters
 *       {@code readsAwaitingApply} in the Raft core, waiting for the
 *       local applied index to reach the recorded commit index.</li>
 *   <li><b>Released</b> -- {@code applied >= commitIndex}. The read
 *       moves from {@code readsAwaitingApply} to {@code readStates}
 *       and is surfaced in the output. The application can now serve
 *       the read.</li>
 * </ol>
 *
 * <h2>Two-Queue Model</h2>
 * <p>The tracker maintains two FIFO queues that mirror the Raft
 * core's two-stage progression. A future exists in exactly one
 * location at any time: pending, confirmed, or completed.</p>
 *
 * <pre>
 *   ReadTracker                                  Raft Core
 *   ───────────                                  ─────────
 *
 *   submit(F1)     submit(F2)     submit(F3)     submit(F4)     submit(F5)
 *     │               │              │              │              │
 *     ▼               ▼              ▼              ▼              ▼
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ pending:  [ F3, F4, F5 ]                                         │
 *   │           unconfirmed -- heartbeat ack not yet received          │
 *   │           failed on role change                                  │
 *   └──────────────────────────────────────────────────────────────────┘
 *                 │
 *                 │  heartbeat majority ack (leadership proven)
 *                 │  F1, F2 moved to confirmed in previous reconcile
 *                 ▼
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ confirmed: [ (F1, idx=10), (F2, idx=12) ]                        │
 *   │            leadership proven -- guaranteed to complete           │
 *   │            preserved across role changes                         │
 *   └──────────────────────────────────────────────────────────────────┘
 *                 │
 *                 │  readStates arrive (applied >= commitIndex)
 *                 ▼
 *              F1.complete(null)  -- application can serve the read
 *
 *
 *   Raft Core (parallel state):
 *
 *   readsAwaitingApply: [ RS(10), RS(12) ]     applied = 9
 *                         ↑                    ─────────────
 *                         next to release      RS(10) released when applied reaches 10
 *                         when applied >= 10   RS(12) released when applied reaches 12
 *
 *   After applied advances to 10:
 *
 *   readsAwaitingApply: [ RS(12) ]             readStates: [ RS(10) ]
 *                                              ────────────────────────
 *                                              surfaced in output, triggers
 *                                              F1.complete(null) in tracker
 * </pre>
 *
 * <h2>Delta Formula</h2>
 * <p>The tracker does not observe individual read events; it infers
 * how many reads were newly confirmed by comparing queue sizes across
 * consecutive {@link #reconcile} calls:</p>
 * <pre>
 *   newlyConfirmed = currentAwaitingApply - previousAwaitingApply + readStates.size()
 *
 *   Derivation:
 *     Let E = reads that entered readsAwaitingApply (newly confirmed)
 *     Let L = reads that left readsAwaitingApply (released to readStates)
 *
 *     currentSize - previousSize = E - L
 *     readStates.size()          = L
 *
 *     E = currentSize - previousSize + L
 *       = currentSize - previousSize + readStates.size()
 * </pre>
 *
 * <h2>Immediately Released Reads</h2>
 * <p>A read may be confirmed and released within the same
 * {@code raft.step()} call if {@code applied >= commitIndex} at the
 * time the read enters {@code readsAwaitingApply}. Such reads appear
 * in {@code readStates} but never appear in the
 * {@code readsAwaitingApply} snapshot (they entered and exited before
 * advance). The tracker handles them by pulling directly from
 * {@code pending} during release (see {@link #releaseImmediateReads}).
 * Monotonic read indices guarantee that immediately released reads
 * always appear after previously confirmed reads in readStates, so
 * {@code releaseConfirmedReads} is called first.</p>
 *
 * <h2>FIFO Ordering Guarantee</h2>
 * <p>Correctness depends on the Raft core's monotonically
 * non-decreasing read indices. Because {@code releaseAppliedReads()}
 * drains from the front of {@code readsAwaitingApply} and stops at
 * the first entry exceeding {@code applied}, older reads are always
 * released before newer ones. This means:</p>
 * <ul>
 *   <li>In {@code readStates}: old released reads precede new ones.</li>
 *   <li>In {@code confirmed}: old futures precede new ones.</li>
 *   <li>Popping from both in order produces correct matches.</li>
 * </ul>
 *
 * <h2>Role Change Behavior</h2>
 * <p>On a volatile state change (role transition), only
 * {@code pending} futures are failed -- these reads were never
 * confirmed and may never be. {@code confirmed} futures are
 * preserved because they represent leadership-proven reads that
 * will eventually complete when applied catches up.</p>
 *
 * <p>A notable case: a follower promoted to leader. The pending
 * futures (awaiting ReadIndexResponse from the old leader) are
 * failed because that response will never arrive. The application
 * can retry these reads against the new leader. Recovering pending
 * reads automatically would require Raft algorithm-level support:
 * the new leader would need to re-register inherited reads and
 * confirm its own authority via heartbeat majority before serving
 * them. This is not currently implemented -- the retry cost is low
 * since leader elections are infrequent.</p>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>A future exists in exactly one location: pending, confirmed,
 *       or completed (removed from both queues).</li>
 *   <li>{@code pending.size() + confirmed.size()} equals the number
 *       of in-flight read futures not yet completed or failed.</li>
 *   <li>Read indices in the confirmed queue are monotonically
 *       non-decreasing (inherited from the Raft core).</li>
 *   <li>{@code previousAwaitingApply} is always consistent with the
 *       last observed {@code readsAwaitingApply.size()}, ensuring
 *       the delta formula produces non-negative results.</li>
 *   <li>{@code newlyConfirmedUnreleasedReads <= readsAwaitingApply.size()}
 *       -- the number of reads to confirm never exceeds items
 *       available at the tail of readsAwaitingApply.</li>
 * </ul>
 *
 * <h2>Safety Checks</h2>
 * <ul>
 *   <li>{@code releaseConfirmedReads}: verifies that the commit index
 *       stored in the confirmed future matches the commit index in the
 *       corresponding {@code ReadState}. A mismatch indicates a FIFO
 *       violation -- throws {@code IllegalStateException}.</li>
 *   <li>{@code confirmReads}: verifies that the count of reads to
 *       confirm does not exceed {@code readsAwaitingApply} size. A
 *       violation indicates a bug in the delta formula -- throws
 *       {@code IllegalStateException}.</li>
 * </ul>
 */
public class ReadTracker {
    private record ConfirmedRead(CompletableFuture<Void> future, long commitIndex) {
    }

    // Futures for unconfirmed reads (heartbeat ack not yet received)
    private final Queue<CompletableFuture<Void>> pending;

    // Futures for confirmed reads, paired with their commit index for safety verification
    private final Queue<ConfirmedRead> confirmed;

    // Size of readsAwaitingApply at the end of the previous reconcile call (for delta computation)
    private int previousAwaitingApply;

    public ReadTracker() {
        pending = new ArrayDeque<>();
        confirmed = new ArrayDeque<>();
        previousAwaitingApply = 0;
    }

    /**
     * Enqueues a future for a newly submitted read request.
     * Called when the read is accepted by the Raft core (no rejection).
     * The future starts in the pending queue and will move to confirmed
     * once the Raft core proves leadership via heartbeat majority.
     */
    public void submit(CompletableFuture<Void> future) {
        pending.add(future);
    }

    /**
     * Reconciles tracked futures against the Raft output produced by
     * a single {@code engine.advance()} call.
     *
     * <p>Executes a four-step pipeline:</p>
     * <ol>
     *   <li><b>Release confirmed</b> -- complete futures from the
     *       confirmed queue that match released readStates.</li>
     *   <li><b>Release immediate</b> -- complete futures from the
     *       pending queue for reads that were confirmed and released
     *       within the same step (never entered confirmed queue).</li>
     *   <li><b>Confirm</b> -- move newly confirmed (but unreleased)
     *       futures from pending to confirmed, pairing each with its
     *       commit index from the tail of readsAwaitingApply.</li>
     *   <li><b>Fail pending on state change</b> -- if a role
     *       transition occurred, fail all remaining pending futures
     *       (unconfirmed reads that may never complete).</li>
     * </ol>
     *
     * @param readsAwaitingApply snapshot of reads confirmed but waiting
     *        for applied to catch up (from Raft core)
     * @param readStates reads that are both confirmed and applied,
     *        ready for the application to serve
     * @param volatileState present if a role change occurred in this
     *        step; triggers failure of pending futures
     */
    public void reconcile(
            List<ReadState> readsAwaitingApply,
            List<ReadState> readStates,
            Optional<VolatileState> volatileState) {

        var newlyConfirmedReads = readsAwaitingApply.size() - previousAwaitingApply + readStates.size();
        previousAwaitingApply = readsAwaitingApply.size();

        var confirmedReleased = releaseConfirmedReads(readStates);

        var immediatelyReleased = readStates.size() - confirmedReleased;
        releaseImmediateReads(immediatelyReleased);

        var newlyConfirmedUnreleasedReads = newlyConfirmedReads - immediatelyReleased;
        confirmReads(readsAwaitingApply, newlyConfirmedUnreleasedReads);

        failPendingOnStateChange(volatileState);
    }

    /**
     * Fails all pending (unconfirmed) futures on a role transition.
     *
     * <p>Only pending futures are affected. Confirmed futures are
     * preserved -- they represent reads proven safe by the previous
     * leader's heartbeat majority and will complete when applied
     * catches up, regardless of the current node's role.</p>
     */
    private void failPendingOnStateChange(Optional<VolatileState> volatileState) {
        if (volatileState.isEmpty()) {
            return;
        }

        while (!pending.isEmpty()) {
            pending.poll().completeExceptionally(
                    new StateChangeException(volatileState.get()));
        }
    }

    /**
     * Moves newly confirmed (but not yet released) futures from
     * pending to confirmed, pairing each with its commit index.
     *
     * <p>The newly confirmed reads correspond to the <b>tail</b> of
     * {@code readsAwaitingApply}. Older items at the head were already
     * confirmed in a previous reconcile call. The tail position is
     * computed as {@code size - confirmedReads}.</p>
     *
     * <p>The commit index stored in each {@link ConfirmedRead} serves
     * as a safety net: when the read is eventually released, it is
     * verified against the corresponding {@code ReadState.index()} to
     * detect any FIFO ordering violation.</p>
     *
     * @param readsAwaitingApply current snapshot of confirmed reads
     *        waiting for applied to catch up
     * @param confirmedReads number of newly confirmed reads that are
     *        still in readsAwaitingApply (excludes immediately released)
     */
    private void confirmReads(List<ReadState> readsAwaitingApply, int confirmedReads) {
        if (confirmedReads > readsAwaitingApply.size()) {
            throw new IllegalStateException(
                    "Confirmed read count (" + confirmedReads
                            + ") exceeds readsAwaitingApply size (" + readsAwaitingApply.size() + ")");
        }
        for (var i = readsAwaitingApply.size() - confirmedReads; i < readsAwaitingApply.size(); i++) {
            confirmed.add(new ConfirmedRead(pending.poll(), readsAwaitingApply.get(i).index()));
        }
    }


    /**
     * Completes confirmed futures whose reads have been released
     * (applied >= commitIndex).
     *
     * <p>Iterates {@code readStates} front-to-back, popping from the
     * confirmed queue in lockstep. Stops when the confirmed queue is
     * empty -- any remaining readStates are immediately released reads
     * that bypassed the confirmed queue entirely.</p>
     *
     * <p>The commit index stored at confirm time is verified against
     * the readState index. Because both queues are FIFO and read
     * indices are monotonically non-decreasing, they must match. A
     * mismatch indicates a FIFO violation in the Raft core or a bug
     * in the delta formula.</p>
     *
     * @param readStates reads released in this advance (confirmed and
     *        applied)
     * @return number of futures completed from the confirmed queue
     */
    private int releaseConfirmedReads(List<ReadState> readStates) {
        int released = 0;
        for (var read : readStates) {
            if (confirmed.isEmpty()) break;
            var confirmedRead = confirmed.poll();
            if (confirmedRead.commitIndex() != read.index()) {
                throw new IllegalStateException(
                        "Read FIFO invariant violated: expected commit index "
                                + confirmedRead.commitIndex() + " but got " + read.index());
            }
            confirmedRead.future().complete(null);
            released++;
        }
        return released;
    }

    /**
     * Completes futures for reads that were confirmed and released
     * within the same {@code raft.step()} call.
     *
     * <p>These reads entered and exited {@code readsAwaitingApply}
     * before {@code advance()} took its snapshot, so they appear in
     * {@code readStates} but not in {@code readsAwaitingApply}. Their
     * futures were never moved to the confirmed queue, so they are
     * pulled directly from pending.</p>
     *
     * <p>This is only possible when {@code applied >= commitIndex} at
     * the moment the read is confirmed. Monotonic read indices
     * guarantee that immediately released reads always appear
     * <b>after</b> previously confirmed reads in readStates, so
     * this method is called after {@link #releaseConfirmedReads}.</p>
     *
     * @param count number of immediately released reads
     */
    private void releaseImmediateReads(int count) {
        for (int i = 0; i < count; i++) {
            Objects.requireNonNull(pending.poll()).complete(null);
        }
    }
}
