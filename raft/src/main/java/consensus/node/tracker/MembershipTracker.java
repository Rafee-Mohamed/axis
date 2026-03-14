package consensus.node.tracker;

import consensus.engine.RaftInput;
import consensus.engine.VolatileState;
import consensus.node.StateChangeException;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Tracks the {@link CompletableFuture} for a locally proposed
 * membership change (enter-joint or leave-joint), completing it
 * when the change is applied to the Raft algorithm.
 *
 * <h2>Why Tracking is Needed</h2>
 * <p>A membership change is not effective until it is applied to
 * the Raft algorithm (not just committed). The algorithm must
 * update its internal configuration before it can use the new
 * quorum, send messages to new peers, or trigger auto-leave-joint.
 * Completing the future on apply tells the client the change is
 * fully in effect.</p>
 *
 * <h2>State Transitions</h2>
 * <pre>
 *                submit(F)                  entry in               ApplyMembership
 *                                         entriesToApply           stepped into Raft
 *   empty -----------------> pending(F) -----------------> confirmed(F) -----------------> F.complete(null)
 *                                 |                            |                            back to empty
 *                                 | state change               | state change
 *                                 v                            v
 *                        F.completeExceptionally()       preserved (no-op)
 *                           back to empty
 * </pre>
 *
 * <h2>When Pending and Confirmed are Empty</h2>
 * <p>Membership entries appear in {@code entriesToApply} and
 * trigger {@code ApplyMembership}/{@code ApplyLeaveJoint} on
 * every node in the cluster, not just the node whose client
 * proposed the change. The tracker only has work to do when
 * a local client called {@code propose()} or
 * {@code proposeLeaveJoint()}. On all other nodes, both fields
 * remain empty and all methods are no-ops. Common cases:</p>
 * <ul>
 *   <li>A follower applying a membership change proposed by
 *       a different node's client.</li>
 *   <li>A leader processing a forwarded membership change
 *       that arrived via {@code receive()}, not {@code propose()}.</li>
 *   <li>Auto-leave-joint: the Raft algorithm internally proposes
 *       leave-joint after applying enter-joint. No client call
 *       was made, so no future exists.</li>
 * </ul>
 *
 * <h2>One-at-a-Time Invariant</h2>
 * <p>The Raft algorithm rejects a new membership change if one
 * is already pending ({@code applied < pendingConfigIndex}). This
 * guarantees that at most one future is tracked at any time.
 * {@link #submit} enforces this with an
 * {@code IllegalStateException} if called while pending is
 * already present.</p>
 *
 * <h2>Committed-but-not-Applied on State Change</h2>
 * <p>If a membership entry is committed (replicated to majority)
 * but the leader loses leadership before {@code ApplyMembership}
 * fires, the future moves to confirmed during
 * {@link #reconcile} (the entry appears in
 * {@code entriesToApply} in the same output as the state change).
 * Because {@code confirmIfCommitted} runs before
 * {@code failPendingOnStateChange}, the future is preserved in
 * confirmed and will complete when apply fires. This avoids
 * unnecessary client retries that could trigger a duplicate
 * membership change round.</p>
 */
public class MembershipTracker {

    /** Future for a locally proposed membership change not yet committed. */
    private Optional<CompletableFuture<Void>> pending;

    /** Future for a committed membership change waiting for apply. */
    private Optional<CompletableFuture<Void>> confirmed;

    public MembershipTracker() {
        this.pending = Optional.empty();
        this.confirmed = Optional.empty();
    }

    /**
     * Stores the future for a locally proposed membership change.
     * Called from {@code track()} when the proposal is accepted
     * by the Raft core (no rejection).
     *
     * @param future the future to complete when the change is applied
     * @throws IllegalStateException if a pending future already exists
     */
    public void submit(CompletableFuture<Void> future) {
        if (pending.isPresent()) {
            throw new IllegalStateException(
                    "Duplicate membership proposal: a pending future already exists");
        }

        pending = Optional.of(future);
    }

    /**
     * Reconciles the tracker against the Raft output.
     *
     * <p>Ordering is critical: confirm must happen before
     * failPending. Both can trigger in the same output (entry
     * committed and leader lost simultaneously). Confirming first
     * ensures the future is preserved in confirmed rather than
     * incorrectly failed.</p>
     *
     * @param entriesToApply committed entries ready for application
     * @param volatileState present if a role change occurred
     */
    public void reconcile(List<Entry> entriesToApply, Optional<VolatileState> volatileState) {
        confirmIfCommitted(entriesToApply);
        failPendingOnStateChange(volatileState);
    }

    /**
     * Completes the confirmed future when the membership change
     * is applied to the Raft algorithm.
     *
     * <p>No-op if confirmed is empty. This is normal when the
     * membership change was not proposed by a local client
     * (remote proposal, forwarded proposal, or auto-leave-joint).</p>
     *
     * @param input the input just stepped into the Raft engine
     */
    public void completeIfMembershipApplied(RaftInput input) {
        if (confirmed.isEmpty()) {
            return;
        }

        if (!(input instanceof RaftInput.ApplyMembership || input instanceof RaftInput.ApplyLeaveJoint)) {
            return;
        }

        confirmed.get().complete(null);
        confirmed = Optional.empty();
    }

    /**
     * Moves pending to confirmed when the membership entry appears
     * in committed entries.
     *
     * <p>No-op if pending is empty. This is normal when the
     * membership change was not proposed by a local client.</p>
     *
     * @param entriesToApply committed entries ready for application
     */
    private void confirmIfCommitted(List<Entry> entriesToApply) {
        if (pending.isEmpty()) {
            return;
        }

        for (var entry: entriesToApply) {
            if (!(entry instanceof Entry.MembershipChange || entry instanceof Entry.LeaveJoint)) {
                continue;
            }

            confirmed = pending;
            pending = Optional.empty();
            break;
        }
    }

    /**
     * Fails the pending future on a role transition.
     *
     * <p>Only pending (uncommitted) futures are failed. Confirmed
     * futures are preserved - the committed entry will be applied
     * regardless of role changes.</p>
     *
     * @param volatileState present if a role change occurred
     */
    private void failPendingOnStateChange(Optional<VolatileState> volatileState) {
        if (pending.isEmpty()) {
            return;
        }

        if (volatileState.isEmpty()) {
            return;
        }

        pending.get().completeExceptionally(new StateChangeException(volatileState.get()));
        pending = Optional.empty();
    }

    /**
     * Fails all tracked futures (both pending and confirmed) with the
     * given cause. Used during node shutdown.
     *
     * @param t the failure cause
     */
    public void failAll(Throwable t) {
        pending.ifPresent(f -> f.completeExceptionally(t));
        confirmed.ifPresent(f -> f.completeExceptionally(t));
    }
}
