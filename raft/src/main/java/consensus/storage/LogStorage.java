package consensus.storage;

import consensus.core.Snapshot;

import java.util.List;

/**
 * Readonly Storage interface for persisted Raft Log.
 *
 * Implemented by the application to provide:
 * - Persisted log entries
 * - Hard state (term, vote, commit)
 * - Snapshots
 *
 * The Storage provides READ access to persisted data.
 * WRITE operations (append, compact) are done by application
 * when processing Ready output.
 *
 * If any method throws StorageException, the Raft instance becomes
 * inoperable. Application must handle recovery.
 */
public interface LogStorage {

    /**
     * Returns the saved persistent state (term, vote, commit) and
     * membership configuration from the most recent snapshot.
     *
     * Called once during Raft initialization.
     */
    InitialState initialState() throws StorageException;

    /**
     * Returns log entries in the range [lo, hi).
     *
     * @param low       start index (inclusive)
     * @param high      end index (exclusive)
     * @param maxSize   maximum total size in bytes (returns at least one entry if any)
     * @return          list of entries (caller owns this list)
     *
     * @throws CompactedException    if lo has been compacted
     * @throws EntryUnavailableException  if entries in range are unavailable
     */
    List<Entry> entries(long low, long high, long maxSize) throws StorageException;

    /**
     * Returns the term of entry at index i.
     *
     * Must support range [firstIndex()-1, lastIndex()].
     * The term at firstIndex()-1 is retained for log matching
     * even if that entry has been compacted.
     *
     * @throws CompactedException    if index has been compacted
     * @throws EntryUnavailableException  if index is beyond last entry
     */
    long term(long index) throws StorageException;

    /**
     * Returns the index of the first available log entry.
     *
     * Entries before this have been compacted into snapshot.
     */
    long firstIndex() throws StorageException;

    /**
     * Returns the index of the last log entry.
     */
    long lastIndex() throws StorageException;

    /**
     * Returns the most recent snapshot.
     *
     * @throws SnapshotUnavailableException  if snapshot is temporarily unavailable
     *         (e.g., being prepared). Raft will retry later.
     */
    Snapshot snapshot() throws StorageException;
}