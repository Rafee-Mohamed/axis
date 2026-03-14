package consensus.storage;

import consensus.core.Snapshot;

import java.util.List;

/**
 * Read-only interface over the persisted Raft log.
 *
 * <p>Implemented by the application to expose persisted log entries,
 * hard state (term, vote), and snapshots. The protocol reads from
 * this interface but never writes to it - write operations (append,
 * compact, apply snapshot) are performed by the application when
 * processing {@link consensus.engine.RaftOutput}.</p>
 *
 * <h2>Persisted Log Structure</h2>
 * <pre>
 *              [Snapshot]
 *                  |
 *                  v
 *  Index:          0          1        2        3        4        5        6
 *          +-------------+--------+--------+--------+--------+--------+--------+
 *          | placeholder |   A    |   B    |   C    |   D    |   E    |   F    |
 *          +-------------+--------+--------+--------+--------+--------+--------+
 *                 ^           ^                                            ^
 *              snapshot       |                                            |
 *               .index        |                                            |
 *                         firstIndex                                  lastIndex
 *
 *          |&lt;-- compact --&gt;|&lt;--------------- available entries ----------------&gt;|
 *             (snapshot)                (returned by entries())
 * </pre>
 *
 * <p>The placeholder at {@code snapshot.index} retains the term for
 * log matching via {@link #term(long)}. Entries before
 * {@code firstIndex()} have been compacted into the snapshot and
 * are no longer available.</p>
 *
 * <p>All methods throw {@link StorageException}. Subtypes such as
 * {@link CompactedException} and {@link EntryUnavailableException}
 * carry specific diagnostics; the Javadoc on each method lists the
 * expected subtypes.</p>
 *
 * @see InMemoryLogStorage
 */
public interface LogStorage {

    /**
     * Returns the persisted hard state and the membership configuration
     * from the most recent snapshot.
     *
     * <p>Called once during Raft initialization to restore the node's
     * term, voted-for, and cluster membership.</p>
     *
     * @return the initial state recovered from storage
     * @throws StorageException if the state cannot be read
     */
    InitialState initialState() throws StorageException;

    /**
     * Returns log entries in the half-open range {@code [low, high)}.
     *
     * <p>The total serialized size of the returned entries is capped at
     * {@code maxSize}, but at least one entry is always returned if any
     * exist in the range.</p>
     *
     * @param low     start index (inclusive)
     * @param high    end index (exclusive)
     * @param maxSize maximum total size in bytes
     * @return list of entries in the range
     * @throws CompactedException        if {@code low} has been compacted
     * @throws EntryUnavailableException if entries in the range are unavailable
     * @throws StorageException          if the read fails
     */
    List<Entry> entries(long low, long high, long maxSize) throws StorageException;

    /**
     * Returns the term of the entry at the given index.
     *
     * <p>Must support the range {@code [firstIndex() - 1, lastIndex()]}.
     * The term at {@code firstIndex() - 1} is retained for log matching
     * even after that entry has been compacted.</p>
     *
     * @param index the log index to query
     * @return the term at that index
     * @throws CompactedException        if the index has been compacted
     * @throws EntryUnavailableException if the index is beyond the last entry
     * @throws StorageException          if the read fails
     */
    long term(long index) throws StorageException;

    /**
     * Returns the index of the first available log entry.
     *
     * <p>Entries before this index have been compacted into a snapshot.</p>
     *
     * @return the first available index
     * @throws StorageException if the read fails
     */
    long firstIndex() throws StorageException;

    /**
     * Returns the index of the last log entry.
     *
     * @return the last index in the log
     * @throws StorageException if the read fails
     */
    long lastIndex() throws StorageException;

    /**
     * Returns the most recent snapshot.
     *
     * @return the current snapshot
     * @throws SnapshotUnavailableException if the snapshot is temporarily
     *         unavailable (e.g., still being prepared) - the protocol will retry
     * @throws StorageException if the read fails
     */
    Snapshot snapshot() throws StorageException;
}