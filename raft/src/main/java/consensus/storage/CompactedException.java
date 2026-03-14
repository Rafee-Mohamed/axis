package consensus.storage;

/**
 * Thrown when a requested log index has been compacted into a snapshot
 * and is no longer available in the log.
 *
 * @see LogStorage#entries(long, long, long)
 * @see LogStorage#term(long)
 */
public class CompactedException extends StorageException {

    /** The log index that was requested but has been compacted. */
    private final long requestedIndex;

    /**
     * Creates a compacted exception for the given index.
     *
     * @param requestedIndex the compacted log index
     */
    public CompactedException(long requestedIndex) {
        super("Index " + requestedIndex + " has been compacted");
        this.requestedIndex = requestedIndex;
    }

    /**
     * Returns the log index that was requested.
     *
     * @return the compacted index
     */
    public long requestedIndex() {
        return requestedIndex;
    }
}
