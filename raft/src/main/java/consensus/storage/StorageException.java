package consensus.storage;

/**
 * Base exception for all storage-related errors.
 *
 * <p>{@link LogStorage} methods declare this type so that callers
 * handle a single checked exception. Subtypes provide finer-grained
 * diagnostics:</p>
 *
 * <ul>
 *   <li>{@link CompactedException} - requested index was compacted</li>
 *   <li>{@link EntryUnavailableException} - requested index is beyond the log</li>
 *   <li>{@link SnapshotUnavailableException} - snapshot is temporarily unavailable</li>
 *   <li>{@link SnapshotOutOfDateException} - snapshot is older than the current one</li>
 * </ul>
 */
public class StorageException extends Exception {

    /**
     * Creates a storage exception with the given message.
     *
     * @param message description of the error
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * Creates a storage exception with the given message and cause.
     *
     * @param message description of the error
     * @param cause   the underlying cause
     */
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
