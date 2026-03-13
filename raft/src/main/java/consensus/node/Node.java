package consensus.node;

import consensus.core.NodeId;
import consensus.core.RaftState;
import consensus.config.RaftConfig;
import consensus.engine.*;
import consensus.cluster.membership.MembershipChanges;
import consensus.cluster.membership.MembershipConfig;
import consensus.message.Message;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.StorageException;

import java.util.*;
import java.util.concurrent.*;
import java.util.random.RandomGenerator;

public class Node<T extends TrackablePayload<ID>, ID> {

    sealed interface Event {
        default RaftInput input() { return null; };
        record Fire(RaftInput input) implements Event {}
        record Awaited(RaftInput input, CompletableFuture<Void> future) implements Event {}
        record Empty() implements Event {}
        record StatusRequest(CompletableFuture<Status> future)  implements Event {}
    }

    private final NodeConfig config;

    private final RaftEngine engine;

    private final BlockingQueue<Event> inbox;
    private final BlockingQueue<WorkItem<T>> outbox;

    private final ReadTracker readTracker;
    private final MembershipTracker membershipTracker;
    private final DataProposalTracker<T, ID> dataProposalTracker;

    private volatile State state;


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

    public CompletableFuture<Void> propose(T data) throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeData(Collections.singletonList(data)), future));
        return future;
    }

    public CompletableFuture<Void> propose(MembershipChanges changes) throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeMembershipChange(changes), future));
        return future;
    }

    public CompletableFuture<Void> proposeLeaveJoint() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeLeaveJoint(), future));
        return future;
    }

    public void tick() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.Tick()));
    }

    public void receive(Message.Peer message) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.Receive(message)));
    }

    public CompletableFuture<Void> readIndex() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ReadIndex(), future));
        return future;
    }

    public void triggerElection() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.TriggerElection()));
    }

    public void reportUnreachablePeer(NodeId id) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ReportUnreachablePeer(id)));
    }

    public void reportSnapshotStatus(NodeId id, boolean success) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ReportSnapshotStatus(id, success)));
    }

    public void transferLeader(NodeId transferee) throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.TransferLeader(transferee)));
    }

    public void forgetLeader() throws InterruptedException {
        if (state != State.RUNNING) { return; }
        inbox.put(new Event.Fire(new RaftInput.ForgetLeader()));
    }

    public boolean shutdown() throws InterruptedException {
        if (state != State.RUNNING) {
            return false;
        }
        state = State.SHUTTING_DOWN;
        inbox.put(new Event.Empty());
        return true;
    }

    public CompletableFuture<Status> status() throws InterruptedException {
        if (state != State.RUNNING) {
            return CompletableFuture.failedFuture(new NodeLifecycleException.ShuttingDown());
        }
        var future = new CompletableFuture<Status>();
        inbox.put(new Event.StatusRequest(future));
        return future;
    }


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

    private void reconcile(RaftOutput output) {
        readTracker.reconcile(output.readsAwaitingApply(), output.readStates(), output.volatileState());
        membershipTracker.reconcile(output.committedEntriesToApply(), output.volatileState());
        dataProposalTracker.reconcile(output.committedEntriesToApply(), output.committedEntriesAwaitingApply(), output.volatileState());
    }

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

    private Optional<WorkItem.Work<T>> advanceAndReconcile() throws StorageException, InterruptedException {
        var output = engine.advance();
        if (output.isEmpty()) {
            return Optional.empty();
        }

        reconcile(output.get());
        var work = buildWork(output.get());
        return Optional.of(work);
    }

    private void advanceAndPublishWhileShutdown(List<Event> events, long deadline) throws StorageException, InterruptedException {
        processEventsOnShutdown(events);
        var work = advanceAndReconcile();

        var remaining = deadline - System.nanoTime();
        if (work.isPresent() && remaining > 0) {
            outbox.offer(work.get(), remaining, TimeUnit.NANOSECONDS);
        }
    }


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

    public WorkItem<T> takeWork() throws InterruptedException {
        return outbox.take();
    }
}
