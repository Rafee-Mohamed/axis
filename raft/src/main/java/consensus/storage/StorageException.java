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
