package consensus.node;

import consensus.algorithm.Snapshot;
import consensus.engine.CheckpointState;
import consensus.engine.PersistentState;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;

public final class PersistTask implements CompletableTask {
    private final Optional<PersistentState> persistentState;
    private final Optional<CheckpointState> checkpointState;
    private final Optional<Snapshot> snapshot;
    private final List<Entry> entriesToPersist;
    private final List<Message.Peer> messagesAfterPersist;
    private final Runnable onComplete;

    public PersistTask(
             PersistentState persistentState,
             CheckpointState checkpointState,
             Snapshot snapshot,
             List<Entry> entriesToPersist,
             List<Message.Peer> messagesAfterPersist,
             Runnable onComplete
    ) {
        this.persistentState = Optional.ofNullable(persistentState);
        this.checkpointState = Optional.ofNullable(checkpointState);
        this.snapshot = Optional.ofNullable(snapshot);
        this.entriesToPersist = entriesToPersist;
        this.messagesAfterPersist = messagesAfterPersist;
        this.onComplete = onComplete;
    }

    @Override
    public void complete() {
        onComplete.run();
    }

    public Optional<PersistentState> persistentState() {
        return persistentState;
    }

    public Optional<CheckpointState> checkpointState() {
        return checkpointState;
    }

    public Optional<Snapshot> snapshot() {
        return snapshot;
    }

    public List<Entry> entriesToPersist() {
        return entriesToPersist;
    }

    public List<Message.Peer> messagesAfterPersist() {
        return messagesAfterPersist;
    }
}
