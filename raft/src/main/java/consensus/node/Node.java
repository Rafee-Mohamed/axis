package consensus.node;

import consensus.algorithm.NodeId;
import consensus.algorithm.RaftState;
import consensus.config.RaftConfig;
import consensus.engine.ExecutionModel;
import consensus.engine.RaftEngine;
import consensus.engine.RaftInput;
import consensus.engine.RaftOutput;
import consensus.membership.MembershipChanges;
import consensus.membership.MembershipConfig;
import consensus.message.Message;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.Payload;
import consensus.storage.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.random.RandomGenerator;

public class Node<T extends Payload> {
    sealed interface Event {
        RaftInput input();
        record Fire(RaftInput input) implements Event {};
        record Awaited(RaftInput input, CompletableFuture<Void> future) implements Event {};
    }

    private final BlockingQueue<Event> inbox;
    private final BlockingQueue<Work<T>> outbox;
    private final RaftEngine engine;

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
    }

    public CompletableFuture<Void> propose(List<T> data) throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeData(data), future));
        return future;
    }

    public CompletableFuture<Void> propose(NodeId from, MembershipChanges changes) throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeMembershipChange(from, changes), future));
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

    public void receive(Message message) throws InterruptedException {
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

    public void transferLeader(NodeId from, NodeId transferee) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.TransferLeader(from, transferee)));
    }

    public void forgetLeader() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ForgetLeader()));
    }

    private Runnable enqueueOnComplete(List<Message> responses) {
        return () -> {
            try {
                inbox.put(new Event.Fire(new RaftInput.Responses(responses)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    private Optional<PersistTask> buildPersistTask(RaftOutput output) {
        if (output.persistentState().isEmpty()
                && output.checkpointState().isEmpty()
                && output.snapshot().isEmpty()
                && output.entriesToPersist().isEmpty()) {
            return Optional.empty();
        }

        var task = new PersistTask(
                output.persistentState().orElse(null),
                output.checkpointState().orElse(null),
                output.snapshot().orElse(null),
                output.entriesToPersist(),
                output.messagesAfterPersist(),
                enqueueOnComplete(output.responsesAfterPersist())
        );

        return Optional.of(task);
    }

    private Optional<ApplyTask<T>> buildApplyTask(RaftOutput output) {
        if (output.entriesToApply().isEmpty()) {
            return Optional.empty();
        }

        var dataEntries = new ArrayList<Entry.Data>();
        RaftInput membershipChangeInput = null;

        for (var entry: output.entriesToApply()) {
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
                inbox.put(new Event.Fire(new RaftInput.Responses(output.responsesAfterApply())));
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
        if (persistTask.isEmpty() && !output.responsesAfterPersist().isEmpty()) {
            inbox.put(new Event.Fire(new RaftInput.Responses(output.responsesAfterPersist())));
        }
        return new Work<>(
                output.volatileState(),
                output.messages(),
                output.readStates(),
                persistTask,
                applyTask
        );
    }

    private void track(Event.Awaited awaited) {
        switch (awaited.input()) {
            case RaftInput.ProposeData _ -> {}
            case RaftInput.ProposeMembershipChange _ -> {}
            case RaftInput.ProposeLeaveJoint _ -> {}
            case RaftInput.ReadIndex _ -> {}
            default -> throw new IllegalStateException("Cannot await RaftInput: " + awaited.input());
        }
    }

    private void process(List<Event> events) throws StorageException {
        for (var event: events) {
            if (event instanceof Event.Awaited awaited) {
                track(awaited);
            }
            engine.process(event.input());
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
