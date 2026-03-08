package consensus.node;

import consensus.algorithm.NodeId;
import consensus.algorithm.RaftState;
import consensus.algorithm.ReadState;
import consensus.algorithm.Rejection;
import consensus.config.RaftConfig;
import consensus.engine.*;
import consensus.membership.MembershipChanges;
import consensus.membership.MembershipConfig;
import consensus.message.Message;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.Payload;
import consensus.storage.StorageException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.random.RandomGenerator;

public class Node<T extends TrackablePayload<ID>, ID> {
    sealed interface Event {
        RaftInput input();
        record Fire(RaftInput input) implements Event {};
        record Awaited(RaftInput input, CompletableFuture<Void> future) implements Event {};
    }

    private final RaftEngine engine;

    private final BlockingQueue<Event> inbox;
    private final BlockingQueue<Work<T>> outbox;

    private final ReadTracker readTracker;
    private final MembershipTracker membershipTracker;
    private final DataProposalTracker<T, ID> dataProposalTracker;


    public Node(
            RaftState state,
            RaftConfig config,
            LogStorage logStorage,
            MembershipConfig membership,
            RandomGenerator random
    ) throws StorageException  {
        inbox = new LinkedBlockingQueue<>();
        var outboxSize = switch (config.executionModel()) {
            case ExecutionModel.SEQUENTIAL -> 1;
            case ExecutionModel.PIPELINED -> 28;
        };
        outbox = new ArrayBlockingQueue<>(outboxSize);
        engine = new RaftEngine(state, config, logStorage, membership, random);
        readTracker = new ReadTracker();
        membershipTracker = new MembershipTracker();
        dataProposalTracker = new DataProposalTracker<>();
    }

    public CompletableFuture<Void> propose(T data) throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeData(Collections.singletonList(data)), future));
        return future;
    }

    public CompletableFuture<Void> propose(MembershipChanges changes) throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeMembershipChange(changes), future));
        return future;
    }

    public CompletableFuture<Void> proposeLeaveJoint() throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeLeaveJoint(), future));
        return future;
    }

    public void tick() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.Tick()));
    }

    public void receive(Message.Peer message) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.Receive(message)));
    }

    public CompletableFuture<Void> readIndex() throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ReadIndex(), future));
        return future;
    }

    public void triggerElection() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.TriggerElection()));
    }

    public void reportUnreachablePeer(NodeId id) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ReportUnreachablePeer(id)));
    }

    public void reportSnapshotStatus(NodeId id, boolean success) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ReportSnapshotStatus(id, success)));
    }

    public void transferLeader(NodeId transferee) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.TransferLeader(transferee)));
    }

    public void forgetLeader() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ForgetLeader()));
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

    private Work<T> buildWork(RaftOutput output) throws InterruptedException {
        var persistTask = buildPersistTask(output);
        var applyTask = buildApplyTask(output);
        if (persistTask.isEmpty() && !output.persistResponses().isEmpty()) {
            inbox.put(new Event.Fire(new RaftInput.PersistResponses(output.persistResponses())));
        }
        return new Work<>(
                output.volatileState(),
                output.messages(),
                persistTask,
                applyTask
        );
    }

    private void process(RaftOutput output) {
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

    private void process(List<Event> events) throws StorageException {
        for (var event: events) {
            var rejection = engine.process(event.input());
            membershipTracker.completeIfMembershipApplied(event.input());
            dataProposalTracker.releaseIfApplied(event.input());

            if (event instanceof Event.Awaited awaited) {
                if (rejection.isPresent()) {
                    awaited.future().completeExceptionally(RejectionException.from(rejection.get()));
                } else {
                    track(awaited);
                }
            }
        }
    }

    public void run() throws StorageException {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                var events = new ArrayList<Event>();
                events.add(inbox.take());
                inbox.drainTo(events);
                process(events);

                var output = engine.advance();
                if (output.isPresent()) {
                    process(output.get());
                    outbox.put(buildWork(output.get()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public Work<T> takeWork() throws InterruptedException {
        return outbox.take();
    }
}
