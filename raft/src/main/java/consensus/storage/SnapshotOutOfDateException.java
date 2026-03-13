package consensus.storage;

/**
 * Snapshot is out of date (older than existing).
 */
class SnapshotOutOfDateException extends StorageException {
    public SnapshotOutOfDateException(long requestedIndex, long existingIndex) {
        super("Snapshot at " + requestedIndex + " is older than existing " + existingIndex);
    }
}
