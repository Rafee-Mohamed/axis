package consensus.node.task;

import consensus.core.Snapshot;
import consensus.engine.CheckpointState;
import consensus.engine.PersistentState;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;

/**
 * A task that carries everything needed for a single persistence cycle:
 * hard state, checkpoint, snapshot, log entries, and peer messages that
 * must be sent only after the write completes.
 *
 * <p>Calling {@link #complete()} delivers the persistence responses back
 * to the engine, advancing the processing loop.</p>
 */
public final class PersistTask implements CompletableTask {

    private final Optional<PersistentState> persistentState;
    private final Optional<CheckpointState> checkpointState;
    private final Optional<Snapshot> snapshot;
    private final List<Entry> entriesToPersist;
    private final List<Message.Peer> messagesAfterPersist;
    private final Runnable onComplete;

    /**
     * @param persistentState      hard state to persist, or {@code null} if unchanged
     * @param checkpointState      checkpoint to persist, or {@code null} if unchanged
     * @param snapshot             snapshot to persist, or {@code null} if none
     * @param entriesToPersist     log entries to write to stable storage
     * @param messagesAfterPersist peer messages to send after persistence completes
     * @param onComplete           callback that delivers responses back to the engine
     */
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

    /** @return hard state to persist, if changed */
    public Optional<PersistentState> persistentState() {
        return persistentState;
    }

    /** @return checkpoint to persist, if changed */
    public Optional<CheckpointState> checkpointState() {
        return checkpointState;
    }

    /** @return snapshot to persist, if any */
    public Optional<Snapshot> snapshot() {
        return snapshot;
    }

    /** @return log entries to write to stable storage */
    public List<Entry> entriesToPersist() {
        return entriesToPersist;
    }

    /** @return peer messages to send after persistence completes */
    public List<Message.Peer> messagesAfterPersist() {
        return messagesAfterPersist;
    }
}
