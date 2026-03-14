package consensus.storage;

/**
 * Thrown when a requested log index is beyond the last entry in the log.
 *
 * @see LogStorage#entries(long, long, long)
 * @see LogStorage#term(long)
 */
public class EntryUnavailableException extends StorageException {

    /** The log index that was requested but is not yet available. */
    private final long requestedIndex;

    /**
     * Creates an entry-unavailable exception for the given index.
     *
     * @param requestedIndex the unavailable log index
     */
    public EntryUnavailableException(long requestedIndex) {
        super("Entry at index " + requestedIndex + " is unavailable");
        this.requestedIndex = requestedIndex;
    }

    /**
     * Returns the log index that was requested.
     *
     * @return the unavailable index
     */
    public long requestedIndex() {
        return requestedIndex;
    }
}
