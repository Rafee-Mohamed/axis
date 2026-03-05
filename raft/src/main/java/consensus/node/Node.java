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
import consensus.storage.LogStorage;
import consensus.storage.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.random.RandomGenerator;

public class Node {
    sealed interface Event {
        RaftInput input();
        record Fire(RaftInput input) implements Event {};
        record Awaited(RaftInput input, CompletableFuture<Void> future) implements Event {};
    }

    private final BlockingQueue<Event> inbox;
    private final BlockingQueue<Work> outbox;
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

    public CompletableFuture<Void> propose(List<byte[]> data) throws InterruptedException {
        var future = new CompletableFuture<Void>();
        inbox.put(new Event.Awaited(new RaftInput.ProposeData(data), future));
        return future;
    }

    public void propose(NodeId from, MembershipChanges changes) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ProposeMembershipChange(from, changes)));
    }

    public void proposeLeaveJoint() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ProposeLeaveJoint()));
    }

    public void tick() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.Tick()));
    }

    public void receive(Message message) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.Receive(message)));
    }

    public void readIndex() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ReadIndex()));
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

    public void applyMembership(MembershipChanges changes) throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ApplyMembership(changes)));
    }

    public void applyLeaveJoint() throws InterruptedException {
        inbox.put(new Event.Fire(new RaftInput.ApplyLeaveJoint()));
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

    private Optional<ApplyTask> buildApplyTask(RaftOutput output) {
        if (output.entriesToApply().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ApplyTask(
                output.entriesToApply(),
                enqueueOnComplete(output.responsesAfterApply())
        ));
    }



    private Work buildWork(RaftOutput output) throws InterruptedException {
        var persistTask = buildPersistTask(output);
        var applyTask = buildApplyTask(output);
        if (persistTask.isEmpty() && !output.responsesAfterPersist().isEmpty()) {
            inbox.put(new Event.Fire(new RaftInput.Responses(output.responsesAfterPersist())));
        }
        return new Work(
                output.volatileState(),
                output.messages(),
                output.readStates(),
                persistTask,
                applyTask
        );
    }

    private void trackAwaited(Event.Awaited awaited) {

    }

    private void process(List<Event> events) throws StorageException {
        for (var event: events) {
            if (event instanceof Event.Awaited awaited) {
                trackAwaited(awaited);
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

    public Work takeWork() throws InterruptedException {
        return outbox.take();
    }
}
