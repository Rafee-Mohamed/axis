package consensus.storage;

/**
 * Thrown when a snapshot being applied is older than the snapshot
 * already held in storage.
 *
 * @see InMemoryLogStorage#applySnapshot(consensus.core.Snapshot)
 */
class SnapshotOutOfDateException extends StorageException {

    /**
     * Creates a snapshot-out-of-date exception.
     *
     * @param requestedIndex the index of the snapshot being applied
     * @param existingIndex  the index of the current snapshot in storage
     */
    public SnapshotOutOfDateException(long requestedIndex, long existingIndex) {
        super("Snapshot at " + requestedIndex + " is older than existing " + existingIndex);
    }
}
