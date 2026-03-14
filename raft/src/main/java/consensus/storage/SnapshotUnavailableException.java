package consensus.storage;

/**
 * Thrown when a snapshot is temporarily unavailable, typically because
 * it is still being prepared. The protocol will retry later.
 *
 * @see LogStorage#snapshot()
 */
public class SnapshotUnavailableException extends StorageException {

    /**
     * Creates a snapshot-unavailable exception.
     */
    public SnapshotUnavailableException() {
        super("Snapshot is temporarily unavailable");
    }
}
