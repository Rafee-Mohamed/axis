package consensus.storage;

/**
 * Base exception for storage errors.
 */
public class StorageException extends Exception {
    public StorageException(String message) {
        super(message);
    }
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
/**
 * Requested index has been compacted (included in snapshot).
 */
public class CompactedException extends StorageException {
    private final long requestedIndex;

    public CompactedException(long requestedIndex) {
        super("Index " + requestedIndex + " has been compacted");
        this.requestedIndex = requestedIndex;
    }

    public long requestedIndex() {
        return requestedIndex;
    }
}

/**
 * Requested entry is not available (index beyond log).
 */
public class EntryUnavailableException extends StorageException {
    private final long requestedIndex;

    public EntryUnavailableException(long requestedIndex) {
        super("Entry at index " + requestedIndex + " is unavailable");
        this.requestedIndex = requestedIndex;
    }

    public long requestedIndex() {
        return requestedIndex;
    }
}

/**
 * Snapshot is temporarily unavailable (being prepared).
 * Raft will retry later.
 */
public class SnapshotUnavailableException extends StorageException {
    public SnapshotUnavailableException() {
        super("Snapshot is temporarily unavailable");
    }
}

/**
 * Snapshot is out of date (older than existing).
 */
class SnapshotOutOfDateException extends StorageException {
    public SnapshotOutOfDateException(long requestedIndex, long existingIndex) {
        super("Snapshot at " + requestedIndex + " is older than existing " + existingIndex);
    }
}


class EntrySizeExceededException extends StorageException {
    public EntrySizeExceededException(long exceededSize) {
        super("Max allowed entries size " + " exceeded - "+ exceededSize);

    }
}
