package consensus.node;

import consensus.core.NodeId;
import consensus.core.RaftState;
import consensus.config.RaftConfig;
import consensus.engine.*;
import consensus.cluster.membership.MembershipChanges;
import consensus.cluster.membership.MembershipConfig;
import consensus.message.Message;
import consensus.node.tracker.DataProposalTracker;
import consensus.node.tracker.MembershipTracker;
import consensus.node.tracker.ReadTracker;
import consensus.node.task.WorkItem;
import consensus.node.task.ApplyTask;
import consensus.node.task.PersistTask;
import consensus.node.tracker.TrackablePayload;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.StorageException;

import java.util.*;
import java.util.concurrent.*;
import java.util.random.RandomGenerator;

/**
 * A thread-safe Raft node built on top of {@link RaftEngine}.
 *
 * <h2>Threading Model</h2>
 * <p>The node uses a single-threaded event loop for all protocol
 * processing. External threads interact with it through the public
 * API methods ({@link #propose}, {@link #tick}, {@link #receive},
 * etc.), which enqueue events onto an unbounded inbox. The event
 * loop consumes these events, steps the engine, reconciles the
 * trackers, and publishes {@link WorkItem}s to a bounded outbox.
 * An application thread consumes work via {@link #takeWork()}.</p>
 *
 * <pre>
 *   Caller threads              Event loop (run())             Application thread
 *   ==============              ==================             ==================
 *
 *   propose(data) --+
 *   tick()        --+      +---------------------------+
 *   receive(msg)  --+----&gt; |         inbox             |
 *   shutdown()    --+      | (unbounded, multi-writer) |
 *                          +-------------+-------------+
 *                                        |
 *                                        v
 *                          +---------------------------+
 *                          | processEvents(batch)      |
 *                          |   engine.process(input)   |
 *                          |   track / reject futures  |
 *                          +-------------+-------------+
 *                                        |
 *                                        v
 *                          +---------------------------+
 *                          | engine.advance()          |
 *                          |   reconcile(trackers)     |
 *                          |   buildWork(output)       |
 *                          +-------------+-------------+
 *                                        |
 *                                        v
 *                          +---------------------------+
 *                          |         outbox            | -----&gt; takeWork()
 *                          |  (bounded, single-writer) |        persist / apply
 *                          +---------------------------+        task.complete()
 *                                                                    |
 *                                        +---------------------------+
 *                                        | responses enqueued
 *                                        v back to inbox
 * </pre>
 *
 * <h2>Inbox and Outbox</h2>
 * <ul>
 *   <li><b>Inbox</b> - multi-producer single-consumer, unbounded
 *       {@link LinkedBlockingQueue}. Caller threads enqueue events;
 *       the event loop is the sole consumer. Events are batched: one
 *       blocking take followed by a drainTo to process all available
 *       events before advancing.</li>
 *   <li><b>Outbox</b> - single-producer single-consumer, bounded
 *       {@link ArrayBlockingQueue}. The event loop produces; the
 *       application thread consumes via {@link #takeWork()}. Size
 *       depends on {@link ExecutionModel}: 1 for {@code SEQUENTIAL}
 *       (natural backpressure), larger for {@code PIPELINED} (allows
 *       pipeline depth).</li>
 * </ul>
 *
 * <h2>Future Tracking</h2>
 * <p>Operations that return a {@link CompletableFuture} (data proposals,
 * membership changes, linearizable reads) are tracked by dedicated
 * trackers that reconcile against each engine output:</p>
 * <ul>
 *   <li>{@link DataProposalTracker} - matches committed entries by ID,
 *       completes futures when entries are applied.</li>
 *   <li>{@link MembershipTracker} - single-slot tracker, completes the
 *       future when the membership change is applied to the protocol.</li>
 *   <li>{@link ReadTracker} - FIFO tracker, completes futures when the
 *       local applied index catches up to the read's commit index.</li>
 * </ul>
 * <p>Pending (uncommitted) futures are failed on role changes. Confirmed
 * (committed) futures are preserved across role changes and completed
 * when the entry is applied. On termination, any futures still tracked
 * are failed - confirmed futures may complete normally during the
 * shutdown loop if their responses arrive before the deadline.</p>
 *
 * <h2>Lifecycle</h2>
 * <p>See {@link State} for the full state machine. {@link #run()}
 * enters the running loop. {@link #shutdown()} transitions to
 * {@code SHUTTING_DOWN} and sends a poison pill. The running loop
 * detects the state change, returns unprocessed events, and the
 * shutdown loop drains remaining work within a deadline. Finally,
 * {@link #run()} terminates - failing all remaining futures and
 * publishing a {@link WorkItem.Terminated} signal.</p>
 *
 * <h2>Shutdown Behavior</h2>
 * <p>During shutdown, only response events ({@code PersistResponses},
 * {@code ApplyResponse}, {@code ApplyMembership},
 * {@code ApplyLeaveJoint}) are processed - new proposals and reads
 * are rejected. This allows in-flight work to complete while refusing
 * new work.</p>
 *
 * @param <T>  the application's payload type
 * @param <ID> the payload's identifier type, used for proposal tracking
 *
 * @see RaftEngine
 * @see WorkItem
 */
public class Node<T extends TrackablePayload<ID>, ID> {

    /**
     * Internal event types enqueued on the inbox.
     */
    sealed interface Event {
        /** Fire-and-forget input - no future tracking. */
        record Fire(RaftInput input) implements Event {}
        /** Input paired with a future - tracked until completion or failure. */
        record Awaited(RaftInput input, CompletableFuture<Void> future) implements Event {}
        /** Poison pill to wake the event loop for shutdown. */
        record Empty() implements Event {}
        /** Status snapshot request, completed on the event loop thread. */
        record StatusRequest(CompletableFuture<Status> future)  implements Event {}
    }

    /** Node configuration (shutdown timeouts). */
    private final NodeConfig config;

    /** The underlying Raft protocol engine. */
    private final RaftEngine engine;

    /** Unbounded event queue - multiple producers, single consumer (event loop). */
    private final BlockingQueue<Event> inbox;

    /** Bounded work queue - single producer (event loop), single consumer (application). */
    private final BlockingQueue<WorkItem<T>> outbox;

    /** Tracks linearizable read futures. */
    private final ReadTracker readTracker;

    /** Tracks membership change futures. */
    private final MembershipTracker membershipTracker;

    /** Tracks data proposal futures by ID. */
    private final DataProposalTracker<T, ID> dataProposalTracker;

    /**
     * Current lifecycle state. Volatile because it is read by external
     * threads (public API methods) and written by the event loop and
     * {@link #shutdown()}.
     */
    private volatile State state;


    /**
     * Creates a new node. Does not start the event loop - call
     * {@link #run()} to begin processing.
     *
     * @param nodeConfig node-level configuration (timeouts)
     * @param raftState  persisted hard state to restore from
     * @param raftConfig raft protocol configuration
     * @param logStorage the log storage implementation
     * @param membership the initial membership configuration
     * @param random     random generator for election timeout jitter
     * @throws StorageException if the log storage cannot be read
     */
    public Node(
            NodeConfig nodeConfig,
            RaftState raftState,
            RaftConfig raftConfig,
            LogStorage logStorage,
            MembershipConfig membership,
            RandomGenerator random
    ) throws StorageException  {
        inbox = new LinkedBlockingQueue<>();
        var outboxSize = switch (raftConfig.executionModel()) {
            case ExecutionModel.SEQUENTIAL -> 1;
            case ExecutionModel.PIPELINED -> 28;
        };
        outbox = new ArrayBlockingQueue<>(outboxSize);
        engine = new RaftEngine(raftState, raftConfig, logStorage, membership, random);
        readTracker = new ReadTracker();
        membershipTracker = new MembershipTracker();
        dataProposalTracker = new DataProposalTracker<>();
        state = State.CREATED;
        config = nodeConfig;
    }

    /* ==================== PUBLIC API ==================== */

    /**
     * Proposes a data entry for replication. The returned future completes
     * when the entry is applied to the state machine.
     *
     * <p>Thread-safe. Can be called from any thread.</p>
     *
     * @param data the payload to replicate
     * @return a future that completes on apply, or fails on rejection/state change
     * @throws InterruptedException if the calling thread is interrupted
     */
    public CompletableFuture<Void> propose(T data) throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeData(Collections.singletonList(data)), future));
        return future;
    }

    /**
     * Proposes a membership configuration change. The returned future
     * completes when the change is applied to the Raft protocol.
     *
     * <p>Thread-safe. Can be called from any thread.</p>
     *
     * @param changes the membership changes to apply
     * @return a future that completes on apply, or fails on rejection/state change
     * @throws InterruptedException if the calling thread is interrupted
     */
    public CompletableFuture<Void> propose(MembershipChanges changes) throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeMembershipChange(changes), future));
        return future;
    }

    /**
     * Proposes leaving joint consensus. The returned future completes
     * when the leave-joint entry is applied.
     *
     * <p>Thread-safe. Can be called from any thread.</p>
     *
     * @return a future that completes on apply, or fails on rejection/state change
     * @throws InterruptedException if the calling thread is interrupted
     */
    public CompletableFuture<Void> proposeLeaveJoint() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeLeaveJoint(), future));
        return future;
    }

    /**
     * Advances all timers by one tick. Should be called at a regular
     * interval by the application's tick scheduler.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void tick() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.Tick()));
    }

    /**
     * Delivers a peer message received from the network.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @param message the peer message
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void receive(Message.Peer message) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.Receive(message)));
    }

    /**
     * Requests a read index for a linearizable read. The returned future
     * completes when the read is safe to serve.
     *
     * <p>Thread-safe. Can be called from any thread.</p>
     *
     * @return a future that completes when the read is safe to serve
     * @throws InterruptedException if the calling thread is interrupted
     */
    public CompletableFuture<Void> readIndex() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ReadIndex(), future));
        return future;
    }

    /**
     * Triggers an election campaign immediately.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void triggerElection() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.TriggerElection()));
    }

    /**
     * Reports that a peer is unreachable.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @param id the unreachable peer
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void reportUnreachablePeer(NodeId id) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ReportUnreachablePeer(id)));
    }

    /**
     * Reports the outcome of a snapshot delivery to a peer.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @param id      the peer the snapshot was sent to
     * @param success {@code true} if delivered, {@code false} if failed
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void reportSnapshotStatus(NodeId id, boolean success) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ReportSnapshotStatus(id, success)));
    }

    /**
     * Requests leadership transfer to a specific node.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @param transferee the node that should become leader
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void transferLeader(NodeId transferee) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.TransferLeader(transferee)));
    }

    /**
     * Clears the known leader reference on this node.
     *
     * <p>Thread-safe. No-op if the node is not running.</p>
     *
     * @throws InterruptedException if the calling thread is interrupted
     */
    public void forgetLeader() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ForgetLeader()));
    }

    /**
     * Initiates graceful shutdown. Transitions the node to
     * {@link State#SHUTTING_DOWN} and sends a poison pill to wake
     * the event loop.
     *
     * <p>Thread-safe. Returns {@code false} if the node is not running.</p>
     *
     * @return {@code true} if shutdown was initiated, {@code false} if
     *         the node was not in the running state
     * @throws InterruptedException if the calling thread is interrupted
     */
    public boolean shutdown() throws InterruptedException {
        if (state != State.RUNNING) {
            return false;
        }
        state = State.SHUTTING_DOWN;
        inbox.put(new Event.Empty());
        return true;
    }

    /**
     * Requests a point-in-time status snapshot. The status is computed
     * on the event loop thread to ensure consistency.
     *
     * <p>Thread-safe. Can be called from any thread.</p>
     *
     * @return a future that completes with the status
     * @throws InterruptedException if the calling thread is interrupted
     */
    public CompletableFuture<Status> status() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Status>();
        inbox.put(new Event.StatusRequest(future));
        return future;
    }


    /* ==================== WORK ASSEMBLY ==================== */

    /**
     * Builds a persistence task from the engine output, if there is
     * anything to persist. The task's onComplete callback enqueues
     * persist responses back into the inbox.
     *
     * @param output the engine output
     * @return a persist task, or empty if nothing to persist
     */
    private Optional<PersistTask> buildPersistTask(RaftOutput output) {
        if (output.persistentState().isEmpty()
                && output.checkpointState().isEmpty()
                && output.snapshot().isEmpty()
                && output.entriesToPersist().isEmpty()) {
            return Optional.empty();
        }

        Runnable onComplete = () -> {
            try {
                inbox.put(new Event.Fire(new RaftInput.PersistResponses(output.persistResponses())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        var task = new PersistTask(
                output.persistentState().orElse(null),
                output.checkpointState().orElse(null),
                output.snapshot().orElse(null),
                output.entriesToPersist(),
                output.messagesAfterPersist(),
                onComplete
        );

        return Optional.of(task);
    }

    /**
     * Builds an apply task from the engine output, if there are
     * committed entries to apply.
     *
     * <p>Separates data entries from membership/leave-joint entries.
     * Membership entries are applied via the onComplete callback
     * (enqueued back to inbox), not handed to the application.
     * If no data entries exist, onComplete runs immediately and
     * no task is returned.</p>
     *
     * @param output the engine output
     * @return an apply task, or empty if no data entries to apply
     */
    private Optional<ApplyTask<T>> buildApplyTask(RaftOutput output) {
        if (output.committedEntriesToApply().isEmpty()) {
            return Optional.empty();
        }

        var dataEntries = new ArrayList<Entry.Data>();
        RaftInput membershipChangeInput = null;

        for (var entry: output.committedEntriesToApply()) {
            switch (entry) {
                case Entry.Data d -> dataEntries.add(d);
                case Entry.MembershipChange mc -> membershipChangeInput = new RaftInput.ApplyMembership(mc.membershipChanges());
                case Entry.LeaveJoint _ -> membershipChangeInput = new RaftInput.ApplyLeaveJoint();
                case Entry.Placeholder _ -> {}
            }
        }

        var finalMembershipChangeInput = membershipChangeInput;
        Runnable onComplete = () -> {
            try {
                if (finalMembershipChangeInput != null) {
                    inbox.put(new Event.Fire(finalMembershipChangeInput));
                }
                if (output.applyResponse().isPresent()) {
                    inbox.put(new Event.Fire(new RaftInput.ApplyResponse(output.applyResponse().get())));
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        if (dataEntries.isEmpty()) {
            onComplete.run();
            return Optional.empty();
        }

        return Optional.of(new ApplyTask<>(
                dataEntries,
                onComplete
        ));
    }

    /**
     * Assembles a work item from the engine output. If no persist task
     * was created but there are persist responses, they are delivered
     * immediately via the inbox (nothing to wait for).
     *
     * @param output the engine output
     * @return the assembled work item
     * @throws InterruptedException if interrupted while enqueuing responses
     */
    private WorkItem.Work<T> buildWork(RaftOutput output) throws InterruptedException {
        var persistTask = buildPersistTask(output);
        var applyTask = buildApplyTask(output);
        if (persistTask.isEmpty() && !output.persistResponses().isEmpty()) {
            inbox.put(new Event.Fire(new RaftInput.PersistResponses(output.persistResponses())));
        }
        return new WorkItem.Work<>(
                output.volatileState(),
                output.messages(),
                persistTask,
                applyTask
        );
    }

    /* ==================== EVENT PROCESSING ==================== */

    /**
     * Routes an accepted awaited event to the appropriate tracker.
     * Called after the engine accepts the input (no rejection).
     *
     * @param awaited the accepted awaited event
     */
    @SuppressWarnings("unchecked")
    private void track(Event.Awaited awaited) {
        switch (awaited.input()) {
            case RaftInput.ProposeData(var data) -> {
                // singleton list
                var trackable = (T) data.getFirst();
                dataProposalTracker.submit(trackable.id(), awaited.future());
            }
            case RaftInput.ProposeMembershipChange _,
                 RaftInput.ProposeLeaveJoint _ -> membershipTracker.submit(awaited.future());
            case RaftInput.ReadIndex _ -> readTracker.submit(awaited.future());
            default -> throw new IllegalStateException("Cannot await RaftInput: " + awaited.input());
        }
    }

    /**
     * Processes a batch of events during normal operation. Each event
     * is stepped through the engine. Awaited events are either tracked
     * (on accept) or their future is failed (on rejection).
     *
     * @param events the batch of events to process
     * @throws StorageException if the engine encounters a storage failure
     */
    private void processEvents(List<Event> events) throws StorageException {
        for (var event: events) {
            switch (event) {
                case Event.Empty _ -> {}
                case Event.StatusRequest(var future) -> future.complete(engine.status());
                case Event.Fire(var input) -> {
                    engine.process(input);
                    membershipTracker.completeIfMembershipApplied(input);
                    dataProposalTracker.releaseIfApplied(input);
                }
                case Event.Awaited awaited -> {
                    var rejection = engine.process(awaited.input());
                    membershipTracker.completeIfMembershipApplied(awaited.input());
                    dataProposalTracker.releaseIfApplied(awaited.input());
                    if (rejection.isPresent()) {
                        awaited.future().completeExceptionally(RejectionException.from(rejection.get()));
                    } else {
                        track(awaited);
                    }
                }

            }
        }
    }

    /**
     * Processes events during shutdown. Only response events
     * (persist/apply responses, membership apply) are processed -
     * new proposals and status requests are rejected, and other
     * fire events are ignored.
     *
     * @param events the batch of events to process
     * @throws StorageException if the engine encounters a storage failure
     */
    private void processEventsOnShutdown(List<Event> events) throws StorageException {
        for (var event: events) {
            switch (event) {
                case Event.Awaited(_,var future) -> future.completeExceptionally(new NodeLifecycleException.ShuttingDown());
                case Event.StatusRequest(var future) -> future.completeExceptionally(new NodeLifecycleException.ShuttingDown());
                case Event.Empty _ -> {} // this is poison pill for shutdown
                case Event.Fire(var input) -> {
                    switch (input) {
                        case RaftInput.ApplyLeaveJoint _,
                             RaftInput.ApplyMembership _,
                             RaftInput.PersistResponses _,
                             RaftInput.ApplyResponse _ -> {
                            engine.process(input);
                            membershipTracker.completeIfMembershipApplied(input);
                            dataProposalTracker.releaseIfApplied(input);
                        }
                        default -> {} // ignore other fire events
                    }
                }
            }
        }
    }

    /* ==================== EVENT LOOP ==================== */

    /**
     * Reconciles all trackers against the engine output - confirming,
     * releasing, or failing futures as appropriate.
     *
     * @param output the engine output
     */
    private void reconcile(RaftOutput output) {
        readTracker.reconcile(output.readsAwaitingApply(), output.readStates(), output.volatileState());
        membershipTracker.reconcile(output.committedEntriesToApply(), output.volatileState());
        dataProposalTracker.reconcile(output.committedEntriesToApply(), output.committedEntriesAwaitingApply(), output.volatileState());
    }

    /**
     * Advances the engine, reconciles trackers, and builds a work item.
     *
     * @return a work item if the engine had output, empty otherwise
     * @throws StorageException if the engine encounters a storage failure
     * @throws InterruptedException if interrupted while enqueuing responses
     */
    private Optional<WorkItem.Work<T>> advanceAndReconcile() throws StorageException, InterruptedException {
        var output = engine.advance();
        if (output.isEmpty()) {
            return Optional.empty();
        }

        reconcile(output.get());
        var work = buildWork(output.get());
        return Optional.of(work);
    }

    /**
     * Processes shutdown events, advances the engine, and publishes
     * work to the outbox with a timed offer (respecting the shutdown
     * deadline).
     *
     * @param events   the batch of events to process
     * @param deadline the shutdown deadline in nanoseconds (absolute)
     * @throws StorageException if the engine encounters a storage failure
     * @throws InterruptedException if interrupted
     */
    private void advanceAndPublishWhileShutdown(List<Event> events, long deadline) throws StorageException, InterruptedException {
        processEventsOnShutdown(events);
        var work = advanceAndReconcile();

        var remaining = deadline - System.nanoTime();
        if (work.isPresent() && remaining > 0) {
            outbox.offer(work.get(), remaining, TimeUnit.NANOSECONDS);
        }
    }


    /**
     * Drains remaining work during shutdown. Processes the events that
     * were in-flight when the running loop exited, then polls the inbox
     * for response events until the deadline expires or the inbox is
     * empty.
     *
     * @param transitionEvents events returned from the running loop
     *                         that were not yet processed
     * @throws InterruptedException if interrupted
     * @throws StorageException if the engine encounters a storage failure
     */
    private void shutdownLoop(List<Event> transitionEvents) throws InterruptedException, StorageException {
        long deadline = System.nanoTime() + config.shutdownTimeout().toNanos();

        advanceAndPublishWhileShutdown(transitionEvents, deadline);

        while (System.nanoTime() < deadline && state == State.SHUTTING_DOWN) {
            var remaining = deadline - System.nanoTime();

            var event = inbox.poll(remaining, TimeUnit.NANOSECONDS);
            if (event == null) {
                break;
            }
            var events = new ArrayList<Event>();
            events.add(event);
            inbox.drainTo(events);

            advanceAndPublishWhileShutdown(events, deadline);
        }
    }


    /**
     * The main event loop. Blocks on the inbox, batches events, processes
     * them through the engine, and publishes work to the outbox. Exits
     * when the state is no longer {@link State#RUNNING}.
     *
     * <p>If shutdown occurs between {@code inbox.take()} and processing,
     * the unprocessed events are returned so the shutdown loop can handle
     * response events among them.</p>
     *
     * @return unprocessed events if shutdown interrupted the loop, empty
     *         list otherwise
     * @throws InterruptedException if interrupted
     * @throws StorageException if the engine encounters a storage failure
     */
    private List<Event> runningLoop() throws InterruptedException, StorageException {
        while (state == State.RUNNING) {
            var events = new ArrayList<Event>();
            events.add(inbox.take());
            inbox.drainTo(events);

            if (state != State.RUNNING) {
                return events;
            }

            processEvents(events);
            var work = advanceAndReconcile();
            if (work.isPresent()) {
                outbox.put(work.get());
            }
        }

        return List.of();
    }

    /**
     * Final cleanup. Fails all remaining tracker futures, publishes a
     * {@link WorkItem.Terminated} signal to the outbox (with a timed
     * offer), and throws the appropriate lifecycle exception.
     *
     * <p>If the thread was interrupted during shutdown, the interrupt
     * flag is restored after the termination signal is published.</p>
     *
     * @param failure the cause of termination, or {@code null} for
     *                graceful shutdown
     * @throws NodeLifecycleException.Termination if terminated due to error
     * @throws NodeLifecycleException.GracefulShutdown if shutdown was clean
     */
    private void terminate(Throwable failure) throws NodeLifecycleException.Termination, NodeLifecycleException.GracefulShutdown {
        state = State.TERMINATING;

        NodeLifecycleException cause = failure == null
                ? new NodeLifecycleException.GracefulShutdown()
                : new NodeLifecycleException.Termination(failure);


        readTracker.failAll(cause);
        membershipTracker.failAll(cause);
        dataProposalTracker.failAll(cause);

        var interrupted = Thread.interrupted();
        try {
            outbox.offer(new WorkItem.Terminated<>(cause), config.workTerminationTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        state = State.TERMINATED;

        switch (cause) {
            case NodeLifecycleException.Termination e -> throw e;
            case NodeLifecycleException.GracefulShutdown e -> throw e;
            default -> {}
        }
    }

    /**
     * Starts the event loop on the calling thread. Blocks until the
     * node shuts down or encounters a fatal error. Must be called
     * exactly once.
     *
     * <p>The calling thread becomes the event loop thread - all engine
     * and tracker operations execute on it. The method always terminates
     * by throwing a lifecycle exception (graceful or fatal).</p>
     *
     * @throws NodeLifecycleException.Termination if a fatal error occurred
     * @throws NodeLifecycleException.GracefulShutdown if shutdown completed cleanly
     * @throws IllegalStateException if the node is not in {@link State#CREATED}
     */
    public void run() throws NodeLifecycleException.Termination, NodeLifecycleException.GracefulShutdown  {
        if (state != State.CREATED) {
            throw new IllegalStateException("Node already started, Current state is " + state);
        }
        state = State.RUNNING;
        Throwable failure = null;
        try {
            var transitionEvents = runningLoop();
            if (state == State.SHUTTING_DOWN) {
                shutdownLoop(transitionEvents);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = e;
        } catch (StorageException e) {
            failure = e;
        }

        terminate(failure);
    }

    /**
     * Blocks until a work item is available from the outbox. Called by
     * the application thread to consume persist/apply tasks and messages.
     *
     * <p>Returns a {@link WorkItem.Terminated} when the node has shut
     * down - the application should stop calling after receiving it.</p>
     *
     * @return the next work item
     * @throws InterruptedException if the calling thread is interrupted
     */
    public WorkItem<T> takeWork() throws InterruptedException {
        return outbox.take();
    }
}
