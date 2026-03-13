package consensus.storage;

/**
 * Snapshot is temporarily unavailable (being prepared).
 * Raft will retry later.
 */
public class SnapshotUnavailableException extends StorageException {
    public SnapshotUnavailableException() {
        super("Snapshot is temporarily unavailable");
    }
}
