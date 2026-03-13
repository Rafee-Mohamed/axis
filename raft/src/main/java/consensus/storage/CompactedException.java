package consensus.storage;

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
