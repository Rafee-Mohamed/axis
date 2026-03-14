package consensus.storage;

/**
 * Application-defined data carried by a {@link Entry.Data} log entry.
 *
 * <p>Implementations must report their serialized size via {@link #bytes()}
 * so the protocol can enforce entry-size limits and batch entries
 * within a transport budget.</p>
 */
public interface Payload {

    /**
     * Returns the serialized size of this payload in bytes.
     *
     * @return size in bytes
     */
    long bytes();
}
