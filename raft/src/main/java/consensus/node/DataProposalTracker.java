package consensus.node;

import consensus.engine.RaftInput;
import consensus.engine.VolatileState;
import consensus.storage.Entry;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks {@link CompletableFuture}s for locally proposed data entries,
 * completing them when the entry is applied to the state machine.
 *
 * <h2>Data Proposal Lifecycle</h2>
 * <p>A data proposal goes through four stages before the client
 * can consider it durable and retrieve results:</p>
 * <ol>
 *   <li><b>Proposed</b> -- the entry is submitted via
 *       {@code node.propose(command)}. On a leader, Raft appends it
 *       to the log. On a follower, Raft forwards it to the leader.</li>
 *   <li><b>Committed</b> -- the entry is replicated to a majority.
 *       It cannot be lost or overwritten. The entry appears in
 *       {@code committedEntriesToApply} or
 *       {@code committedEntriesAwaitingApply} in the Raft output.</li>
 *   <li><b>Applied</b> -- the application processes the entry
 *       through its state machine, producing a result. The
 *       application stores the result (e.g., in a result map)
 *       and calls {@code applyTask.complete()}.</li>
 *   <li><b>Released</b> -- the {@code ApplyResponse} event fires
 *       back into the event loop, advancing Raft's applied index.
 *       The tracker completes the future. The client unblocks and
 *       retrieves the result from the application's result map.</li>
 * </ol>
 *
 * <h2>ID-Based Tracking</h2>
 * <p>Unlike reads (FIFO queue) or membership changes (single slot),
 * data proposals use ID-based tracking via a {@code Map<ID, Future>}.
 * This is necessary because:</p>
 * <ul>
 *   <li>Multiple data proposals can be in-flight concurrently.</li>
 *   <li>Committed entries include both local proposals and entries
 *       from other nodes (forwarded proposals, remote proposals).
 *       A count-based approach cannot distinguish local entries
 *       from remote ones.</li>
 *   <li>IDs are intrinsic to the data -- they survive serialization,
 *       replication, and leader changes. An entry's ID is the same
 *       on every node, regardless of how it arrived.</li>
 * </ul>
 *
 * <h2>Two-Structure Model</h2>
 * <p>The tracker maintains a map for unconfirmed proposals and a
 * deque for confirmed (committed) proposals. A future exists in
 * exactly one location at any time: pending, confirmed, or
 * completed.</p>
 *
 * <p><b>Future lifecycle (state transitions):</b></p>
 * <pre>
 *                submit(id, F)           entry committed           ApplyResponse
 *                                        ID matched in             applied >= entry.index
 *                                        committed entries
 *   not tracked ─────────────► pending ─────────────────► confirmed ─────────────► F.complete(null)
 *                                │                           │
 *                                │ state change              │ state change
 *                                ▼                           ▼
 *                       F.completeExceptionally()      preserved (no-op)
 * </pre>
 *
 * <p><b>Committed entry sources for confirmation:</b></p>
 * <pre>
 *   Raft Log:
 *
 *   ──┬───────┬────────────────┬─────────────────────┬────────────────────────
 *     applied              applying               committed
 *        3                    6                      10
 *
 *              ├──────────────┤ ├─────────────────────┤
 *              committedEntries committedEntries
 *              ToApply          AwaitingApply
 *              [4, 5, 6]        [7, 8, 9, 10]
 *              (bounded)        (unbounded, for tracking)
 *                    │                │
 *                    └───────┬────────┘
 *                            ▼
 *                  confirmNewlyCommitted()
 *                  scan entries, match ID against pending
 *                  if matched: pending.remove(id) --> confirmed.add(index, future)
 * </pre>
 *
 * <p><b>Release on apply:</b></p>
 * <pre>
 *   confirmed deque:  [ (F1, 101), (F2, 103), (F3, 107) ]
 *
 *   ApplyResponse arrives: appliedIndex = 103
 *
 *   drain head while index <= 103:
 *     (F1, 101)  101 <= 103  -->  F1.complete(null)
 *     (F2, 103)  103 <= 103  -->  F2.complete(null)
 *     (F3, 107)  107 > 103   -->  stop
 *
 *   confirmed deque:  [ (F3, 107) ]
 * </pre>
 *
 * <h2>Why Committed Entries in Output</h2>
 * <p>The Raft output provides two disjoint sets of committed
 * entries:</p>
 * <pre>
 *   Log:  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 *   Applied: 3
 *   Applying: 6  (entries 4-6 sent to application, bounded by maxApplyingEntriesSize)
 *   Committed: 10
 *
 *   committedEntriesToApply:        [4, 5, 6]       -- bounded, for application to apply
 *   committedEntriesAwaitingApply:  [7, 8, 9, 10]   -- the rest, for tracking only
 * </pre>
 * <p>Without {@code committedEntriesAwaitingApply}, entries 7-10
 * would remain in {@code pending} even though they are committed
 * (cannot be lost). If a state change occurs before they move into
 * {@code committedEntriesToApply}, their futures would be falsely
 * failed. The application would retry, causing duplicates.</p>
 * <p>By scanning both sets, the tracker confirms all committed
 * proposals regardless of apply backpressure.</p>
 *
 * <h2>Index Optimization</h2>
 * <p>{@code committedEntriesAwaitingApply} is a sliding window
 * that can overlap across advance cycles -- the same entries
 * appear until they move into {@code committedEntriesToApply}.
 * To avoid redundant scanning, {@link #confirmNewlyCommitted}
 * uses the tail of the confirmed deque ({@code confirmed.getLast().index()})
 * as a high-water mark. Since log entries have contiguous indices,
 * the starting position is computed directly:</p>
 * <pre>
 *   startFrom = lastConfirmedIndex - entries.first().index() + 1
 * </pre>
 * <p>Only entries beyond the high-water mark are scanned. When
 * the confirmed deque is empty (all futures released), the scan
 * starts from the beginning. This is harmless because
 * {@code pending.remove(id)} returns null for already-confirmed
 * entries, and the {@code pending.isEmpty()} guard short-circuits
 * entirely when no proposals are in flight.</p>
 *
 * <h2>When Pending and Confirmed are Empty</h2>
 * <p>Committed entries contain proposals from all nodes in the
 * cluster, not just local proposals. The tracker only has futures
 * for entries proposed through this node's {@code propose()} method.
 * For all other entries, {@code pending.remove(id)} returns null
 * and no future is tracked. Common cases where both structures
 * are empty:</p>
 * <ul>
 *   <li>A follower applying data entries proposed by a different
 *       node's client directly to the leader.</li>
 *   <li>A leader applying entries that arrived via
 *       {@code receive()} (forwarded from a follower whose client
 *       is not on this node).</li>
 * </ul>
 *
 * <h2>Role Change Behavior</h2>
 * <p>On a volatile state change (role transition), only
 * {@code pending} futures are failed -- these proposals are not
 * yet committed and may be lost if the log is truncated by a
 * new leader. {@code confirmed} futures are preserved -- the
 * committed entries will be applied regardless of role changes.</p>
 * <p>Ordering in {@link #reconcile} is critical: confirm runs
 * before failPending. Both can trigger in the same output (entries
 * committed and leader lost simultaneously). Confirming first
 * ensures committed proposals are preserved rather than
 * incorrectly failed.</p>
 * <p>Data proposals should be idempotent for Raft correctness.
 * A false failure (due to an uncommitted proposal being lost)
 * causes the client to retry. The state machine handles the
 * duplicate as a no-op.</p>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>A future exists in exactly one location: pending map,
 *       confirmed deque, or completed (removed from both).</li>
 *   <li>Entry indices in the confirmed deque are monotonically
 *       increasing (inherited from the log's index ordering).</li>
 *   <li>{@code releaseApplied} drains from the head of the deque,
 *       releasing entries in log order. The applied index advances
 *       monotonically, so {@code confirmed.peek().index() <= appliedIndex}
 *       always releases the correct prefix.</li>
 *   <li>Each {@code propose()} call produces exactly one entry
 *       (singleton list). This guarantees a 1:1 mapping between
 *       a proposal's ID and its future.</li>
 * </ul>
 */
public class DataProposalTracker<T extends TrackablePayload<ID>, ID> {

    private record ConfirmedProposal(long index, CompletableFuture<Void> future) {};

    // Futures for uncommitted proposals, keyed by the command's ID
    private final Map<ID, CompletableFuture<Void>> pending;

    // Futures for committed proposals awaiting apply, ordered by entry index
    private final Deque<ConfirmedProposal> confirmed;

    public DataProposalTracker() {
        pending = new HashMap<>();
        confirmed = new ArrayDeque<>();
    }

    /**
     * Stores a future for a locally proposed data entry.
     * Called from {@code track()} when the proposal is accepted
     * by the Raft core (no rejection).
     *
     * @param id     the command's unique identifier
     * @param future the future to complete when the entry is applied
     */
    public void submit(ID id, CompletableFuture<Void> future) {
        pending.put(id, future);
    }

    /**
     * Reconciles tracked futures against the Raft output produced
     * by a single {@code engine.advance()} call.
     *
     * <p>Executes a three-step pipeline:</p>
     * <ol>
     *   <li><b>Confirm from committedEntriesToApply</b> -- scan
     *       entries being sent to the application for apply. Match
     *       IDs against pending, move to confirmed deque.</li>
     *   <li><b>Confirm from committedEntriesAwaitingApply</b> --
     *       scan the committed backlog beyond the apply boundary.
     *       Ensures no committed proposal is left in pending due
     *       to apply backpressure.</li>
     *   <li><b>Fail pending on state change</b> -- if a role
     *       transition occurred, fail all remaining pending futures
     *       (uncommitted proposals that may be lost).</li>
     * </ol>
     *
     * <p>Ordering is critical: confirm must run before failPending.
     * A committed entry and a state change can appear in the same
     * output. Confirming first preserves the committed future.</p>
     *
     * @param committedEntriesToApply committed entries released for
     *        apply (bounded by maxApplyingEntriesSize)
     * @param committedEntriesAwaitingApply committed entries beyond
     *        the apply boundary (the backpressure overflow)
     * @param volatileState present if a role change occurred
     */
    public void reconcile(
            List<Entry> committedEntriesToApply,
            List<Entry> committedEntriesAwaitingApply,
            Optional<VolatileState> volatileState
    ) {
        confirmNewlyCommitted(committedEntriesToApply);
        confirmNewlyCommitted(committedEntriesAwaitingApply);
        failPendingOnStateChange(volatileState);
    }

    /**
     * Completes confirmed futures when data entries are applied
     * to the state machine.
     *
     * <p>Called during event processing when an
     * {@link RaftInput.ApplyResponse} event arrives (fired by the
     * apply task's onComplete callback). Drains the confirmed deque
     * from the head, completing all futures whose entry index is
     * at or below the applied index.</p>
     *
     * <p>No-op if confirmed is empty. This is normal when the
     * applied entries contain no locally proposed data (remote
     * proposals, membership entries, placeholders).</p>
     *
     * @param input the input just stepped into the Raft engine
     */
    public void releaseIfApplied(RaftInput input) {
        if (confirmed.isEmpty()) {
            return;
        }

        if (input instanceof RaftInput.ApplyResponse(var msg)) {
            var lastAppliedIndex = msg.entries().getLast().index();
            releaseApplied(lastAppliedIndex);
        }
    }

    /**
     * Drains confirmed futures up to the given applied index.
     * Since confirmed entries are ordered by index and the applied
     * index advances monotonically, this always releases the
     * correct prefix of the deque.
     *
     * @param appliedIndex the highest entry index applied in this batch
     */
    private void releaseApplied(long appliedIndex) {
        while (!confirmed.isEmpty() && confirmed.peek().index() <= appliedIndex) {
            confirmed.poll().future().complete(null);
        }
    }

    /**
     * Scans committed entries for IDs matching pending proposals,
     * moving matched futures from pending to confirmed.
     *
     * <p>Uses the confirmed deque's tail index as a high-water
     * mark to skip entries already processed in a previous cycle.
     * Since log entries have contiguous indices, the starting
     * position is computed directly rather than iterating from
     * the beginning.</p>
     *
     * <p>Non-data entries (membership changes, placeholders) are
     * skipped. Data entries whose ID is not in pending are also
     * skipped (remote proposals, already-confirmed entries).</p>
     *
     * @param entries committed entries to scan (either
     *        committedEntriesToApply or committedEntriesAwaitingApply)
     */
    private void confirmNewlyCommitted(List<Entry> entries) {
        if (pending.isEmpty() || entries.isEmpty()) {
            return;
        }

        var lastConfirmedIndex = confirmed.isEmpty() ? -1 : confirmed.getLast().index();
        if (lastConfirmedIndex >= entries.getLast().index()) {
            return;
        }

        var entryIndexOfNextCommittedEntry = 0;
        if (lastConfirmedIndex >= entries.getFirst().index()) {
            entryIndexOfNextCommittedEntry = (int) (lastConfirmedIndex - entries.getFirst().index() + 1);
        }

        while (entryIndexOfNextCommittedEntry < entries.size()) {
            confirmEntry(entries.get(entryIndexOfNextCommittedEntry));
            entryIndexOfNextCommittedEntry++;
        }
    }

    /**
     * Confirms a single entry by extracting its ID and matching
     * against the pending map.
     *
     * <p>The cast to {@code T} is safe because all data entries
     * in the log carry {@link TrackablePayload} instances -- the
     * Node's generic bound enforces this at the propose boundary.
     * Entries from remote nodes carry the same concrete type after
     * deserialization.</p>
     *
     * @param entry the committed entry to check
     */
    @SuppressWarnings("unchecked")
    private void confirmEntry(Entry entry) {
        if (entry instanceof Entry.Data dataEntry) {
            var trackable = (T) dataEntry.data();
            var future = pending.remove(trackable.id());
            if (future != null) {
                confirmed.add(new ConfirmedProposal(dataEntry.index(), future));
            }
        }
    }

    /**
     * Fails all pending (uncommitted) futures on a role transition.
     *
     * <p>Only pending futures are affected. Confirmed futures are
     * preserved -- their entries are committed and will be applied
     * regardless of role changes.</p>
     *
     * @param volatileState present if a role change occurred
     */
    private void failPendingOnStateChange(Optional<VolatileState> volatileState) {
        if (volatileState.isEmpty() || pending.isEmpty()) {
            return;
        }

        for (var future: pending.values()) {
            future.completeExceptionally(
                    new StateChangeException(volatileState.get())
            );
        }

        pending.clear();
    }
}
