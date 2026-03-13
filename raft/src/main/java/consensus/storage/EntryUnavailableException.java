package consensus.storage;

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
