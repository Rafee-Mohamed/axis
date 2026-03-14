package consensus.storage;

import consensus.core.Snapshot;
import consensus.config.RaftLogConfig;

import java.util.*;

/**
 * Unified view of the Raft log, combining persisted entries from
 * {@link LogStorage} with in-memory entries from {@link UnstableLog}.
 *
 * <h2>Log Structure</h2>
 * <pre>
 *  [Snapshot]
 *      |
 *      v
 * Index:   0     1     2     3     4     5     6     7     8     9    10    11    12
 *       +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 *       |snap |  A  |  B  |  C  |  D  |  E  |  F  |  G  |  H  |  I  |  J  |  K  |  L  |
 *       +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 *         ^      ^           ^           ^           ^     ^                 ^       ^
 *      snapshot  |        applied     applying   committed |          persistingUpTo |
 *       .index   |                                         |                         |
 *            firstIndex                                  offset                  lastIndex
 *
 *       |&lt;----------------- LogStorage -------------------&gt;|&lt;----- UnstableLog -----&gt;|
 *                        (persisted)                           (in-memory)
 * </pre>
 *
 * <h2>Entry Ranges</h2>
 * <pre>
 *   Range                       Description
 *   --------------------------------------------------------------------------
 *   [0, firstIndex)             Compacted into snapshot, no longer available
 *   [firstIndex, applied]       Applied to state machine
 *   (applied, applying]         Sent to application, being applied (in-flight)
 *   (applying, committed]       Committed (safe to apply), waiting in queue
 *   (committed, offset)         Persisted but not yet committed (waiting for quorum)
 *   [offset, persistingUpTo)    In UnstableLog, currently being persisted
 *   [persistingUpTo, lastIndex] In UnstableLog, not yet sent for persistence
 * </pre>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>{@code applied &lt;= applying &lt;= committed &lt;= lastIndex}</li>
 *   <li>{@code firstIndex == snapshot.index + 1}</li>
 *   <li>{@code offset &lt;= persistingUpTo &lt;= lastIndex + 1}</li>
 *   <li>{@code committed} never decreases</li>
 *   <li>{@code applied} never decreases</li>
 * </ul>
 *
 * <h2>Entry Lifecycle</h2>
 * <ol>
 *   <li>Entry arrives - goes to UnstableLog (after persistingUpTo)</li>
 *   <li>Output generated - entry included for persistence, persistingUpTo advances</li>
 *   <li>Application persists - {@link #stableTo} called, entry removed from UnstableLog, offset advances</li>
 *   <li>Quorum reached - committed advances</li>
 *   <li>Output generated - entry included in committed entries, applying advances</li>
 *   <li>Application applies to state machine - {@link #appliedTo} called, applied advances</li>
 *   <li>Snapshot taken - entries compacted, firstIndex advances</li>
 * </ol>
 */
public class RaftLog {

    /** Persisted log storage (read-only interface). */
    private final LogStorage log;

    /** In-memory entries not yet persisted. */
    private final UnstableLog unstableLog;

    /** Highest index known to be replicated to a quorum. */
    private long committed;

    /**
     * Highest index that has been sent to the application for applying.
     * Invariant: {@code applied <= applying <= committed}.
     */
    private long applying;

    /**
     * Highest index that has been applied to the state machine.
     * Invariant: {@code applied <= committed}.
     */
    private long applied;

    /** Log configuration (backpressure limits, appliable entries policy). */
    private final RaftLogConfig config;

    /** Current aggregate size of entries being applied (backpressure tracking). */
    private long applyingEntriesSize;

    /** {@code true} when entry application is paused due to the size limit. */
    private boolean applyingEntriesPaused;

    /**
     * Creates a RaftLog backed by the given storage.
     * Initializes committed/applying/applied to the last compacted index
     * (the index just before firstIndex, which is the snapshot boundary).
     *
     * @param log the persistent log storage
     * @param config maximum bytes of entries that can be in-flight for applying
     * @throws StorageException if storage cannot be read
     */
    public RaftLog(LogStorage log, RaftLogConfig config) throws StorageException {
        var lastCompactedIndex = log.firstIndex() - 1;
        var lastStorageIndex = log.lastIndex();
        this.log = log;
        this.committed = lastCompactedIndex;
        this.applying = lastCompactedIndex;
        this.applied = lastCompactedIndex;
        this.config = config;
        this.unstableLog = new UnstableLog(lastStorageIndex);
    }

    /* ==================== GETTERS ==================== */

    /**
     * Returns the highest committed index.
     *
     * @return the committed index
     */
    public long committed() { return committed; }

    /**
     * Returns the highest applied index.
     *
     * @return the applied index
     */
    public long applied() { return applied; }

    /* ==================== INDEX QUERIES ==================== */

    /**
     * Returns the first available log index.
     *
     * <p>If an unstable snapshot exists, returns
     * {@code snapshot.index + 1}. Otherwise delegates to storage.</p>
     *
     * @return the first available index
     * @throws StorageException if the storage read fails
     */
    public long firstIndex() throws StorageException {
        return unstableLog.firstIndex().orElse(log.firstIndex());
    }

    /**
     * Returns the last log index. Checks unstable entries first,
     * then falls back to storage.
     *
     * @return the last index in the log
     * @throws StorageException if the storage read fails
     */
    public long lastIndex() throws StorageException {
        return unstableLog.lastIndex().orElse(log.lastIndex());
    }

    /**
     * Returns the term of the entry at the given index.
     * Checks unstable log first to avoid disk read if entry is still in memory.
     *
     * @param index the log index to query
     * @return the term at the index
     * @throws CompactedException if index has been compacted
     * @throws EntryUnavailableException if index is beyond lastIndex
     */
    public long term(long index) throws StorageException {
        var unstableLogTerm = unstableLog.term(index);

        if (unstableLogTerm.isPresent())
            return unstableLogTerm.getAsLong();

        var logFirstIndex = log.firstIndex();

        if (index < logFirstIndex - 1)
            throw new CompactedException(index);

        if (index > lastIndex())
            throw new EntryUnavailableException(index);

        return log.term(index);
    }

    /**
     * Returns the {@link Entry.Id} (term, index) of the last entry.
     *
     * @return the last entry's id
     * @throws StorageException if the storage read fails
     */
    public Entry.Id lastEntryId() throws StorageException {
        var lastIndex = lastIndex();
        return new Entry.Id(term(lastIndex), lastIndex);
    }

    /* ==================== LOG MATCHING ==================== */

    /**
     * Returns {@code true} if the log contains an entry at the given
     * index with the given term.
     *
     * @param term  the expected term
     * @param index the log index to check
     * @return {@code true} if the entry exists and the term matches
     */
    public boolean matchTerm(long term, long index) {
        try {
            return term(index) == term;
        } catch(StorageException e) {
            return false;
        }
    }

    /**
     * Returns entries starting from the given index, up to
     * {@code maxSize} bytes.
     *
     * @param index   starting index
     * @param maxSize maximum total size in bytes
     * @return entries in the range, or empty if {@code index > lastIndex}
     * @throws StorageException if the storage read fails
     */
    public List<Entry> entries(long index, long maxSize) throws StorageException {
        var lastIndex = lastIndex();
        if (index > lastIndex)
            return List.of();

        return slice(index, lastIndex + 1, maxSize);
    }

    /**
     * Returns all entries in the log. Primarily for debugging and testing.
     *
     * @return all log entries
     * @throws StorageException if the storage read fails
     */
    public List<Entry> allEntries() throws StorageException {
        return entries(firstIndex(), Long.MAX_VALUE);
    }

    /**
     * Attempts to append entries from the leader, verifying log
     * consistency first.
     *
     * <ol>
     *   <li>Check if we have an entry at {@code prevIndex} with
     *       {@code prevTerm} (log consistency check).</li>
     *   <li>If not, return empty - the leader will retry with earlier
     *       entries.</li>
     *   <li>Find the first conflicting entry (same index, different
     *       term) and truncate from that point.</li>
     *   <li>Append new entries and advance committed to
     *       {@code min(leaderCommitIndex, newLastIndex)}.</li>
     * </ol>
     *
     * @param prevTerm          term of the entry at {@code prevIndex}
     * @param prevIndex         index immediately preceding the new entries
     * @param entries           new entries to append (may be empty for heartbeat)
     * @param leaderCommitIndex the leader's committed index
     * @return the new last index if successful, or empty if the log
     *         does not match
     * @throws StorageException      if the storage read fails
     * @throws IllegalStateException if a conflict is detected with an
     *                               already committed entry
     */
    public OptionalLong tryAppend(long prevTerm, long prevIndex, List<Entry> entries, long leaderCommitIndex) throws StorageException, IllegalStateException {
        if (!matchTerm(prevTerm, prevIndex))
            return OptionalLong.empty();

        var newLastIndex = prevIndex + entries.size();
        var conflictIndex = findConflictIndex(entries);

        if (conflictIndex.isPresent() && conflictIndex.getAsLong() <= committed) {
            throw new IllegalStateException(
                "entry " + conflictIndex + " conflicts with committed entry [committed=" + committed + "]");
        } else if (conflictIndex.isPresent()) {
            var offset = prevIndex + 1;
            var nonConflictingEntries = (int) (conflictIndex.getAsLong() - offset);
            if (nonConflictingEntries > entries.size()) {
                throw new IllegalStateException(
                    "conflict index " + conflictIndex + " out of range, offset=" + offset + ", entries.size=" + entries.size());
            }
            append(entries.subList(nonConflictingEntries, entries.size()));
        }
        commitTo(Math.min(newLastIndex, leaderCommitIndex));
        return OptionalLong.of(newLastIndex);
    }

    /**
     * Appends entries to the log via the unstable log.
     *
     * @param entries entries to append
     * @return the new last index
     * @throws StorageException         if the storage read fails
     * @throws IllegalArgumentException if entries would overwrite
     *                                  committed entries
     */
    public long append(List<Entry> entries) throws StorageException {
        if (entries.isEmpty())
            return lastIndex();

        var precedingIndex = entries.getFirst().index() - 1;

        if (precedingIndex < committed)
            throw new IllegalArgumentException(
                "preceding index " + precedingIndex + " is out of range [committed=" + committed + "]");

        unstableLog.append(entries);

        return lastIndex();
    }

    /**
     * Finds the index of the first entry that conflicts with the
     * existing log. An entry conflicts if it has the same index but a
     * different term, or if the index does not exist in the log.
     *
     * @param entries entries to check
     * @return the conflict index, or empty if all entries match
     */
    public OptionalLong findConflictIndex(List<Entry> entries) {
        for (var entry: entries) {
            if (!matchTerm(entry.term(), entry.index())) {
                return OptionalLong.of(entry.index());
            }
        }

        return OptionalLong.empty();
    }

    /**
     * Finds the best guess for where our log might match another log,
     * given only the (term, index) of their entry.
     *
     * <p>Returns the highest index {@code <= index} where our term
     * {@code <= term}. This optimization helps skip over entire terms
     * during log reconciliation after a rejection.</p>
     *
     * @param term  the term from the rejected entry
     * @param index the index from the rejected entry
     * @return the (term, index) of the best matching point
     */
    public Entry.Id findConflictEntryByTerm(long term, long index) {
        try {
            while (index > 0) {
                var termAtIndex = term(index);
                if (termAtIndex <= term)
                    return new Entry.Id(termAtIndex, index);
                index--;
            }
        } catch (StorageException e) {
            return new Entry.Id(0, index);
        }

        return new Entry.Id(0, 0);
    }

    /* ==================== OUTPUT GENERATION ==================== */

    /**
     * Returns entries that need to be persisted (from the unstable log).
     *
     * @return entries pending persistence
     */
    public List<Entry> nextEntriesToPersist() {
        return unstableLog.nextEntriesToPersist();
    }

    /**
     * Returns {@code true} if there are entries waiting to be persisted.
     *
     * @return {@code true} if persistence is pending
     */
    public boolean hasEntriesToPersist() {
        return unstableLog.entriesToPersistCount() > 0;
    }

    /**
     * Returns {@code true} if there are any entries in the unstable log
     * (either pending or in-progress).
     *
     * @return {@code true} if unstable entries exist
     */
    public boolean hasUnstableEntries() {
        return unstableLog.totalEntriesCount() > 0;
    }

    /**
     * Returns {@code true} if there is an unstable snapshot waiting
     * to be persisted.
     *
     * @return {@code true} if a snapshot is pending
     */
    public boolean hasUnstableSnapshot() {
        return unstableLog.nextSnapshot() != null;
    }

    /**
     * Returns {@code true} if a snapshot is currently being persisted.
     *
     * @return {@code true} if snapshot persistence is in progress
     */
    public boolean isSnapshotInProgress() {
        return unstableLog.snapshotInProgress();
    }

    /**
     * Returns the current snapshot. Checks the unstable log first
     * (incoming snapshot from leader), then falls back to storage.
     *
     * @return the current snapshot
     * @throws StorageException if the storage read fails
     */
    public Snapshot snapshot() throws StorageException {
        if (unstableLog.nextSnapshot() != null)
            return unstableLog.nextSnapshot();

        return log.snapshot();
    }

    /**
     * Returns the pending unstable snapshot, or {@code null} if none.
     *
     * @return the unstable snapshot, or {@code null}
     */
    public Snapshot nextUnstableSnapshot() {
        return unstableLog.nextSnapshot();
    }

    /**
     * Returns committed entries that are ready to be applied to the
     * state machine, in the range {@code (applying, maxAppliableIndex]}.
     *
     * <p>Returns empty if application is paused due to backpressure,
     * a pending snapshot must be applied first, or there are no new
     * entries to apply.</p>
     *
     * @return entries to apply, respecting size limits
     * @throws StorageException if the storage read fails
     */
    public List<Entry> nextCommittedEntries() throws StorageException {
        if (applyingEntriesPaused || hasUnstableSnapshot())
            return List.of();

        var low = applying + 1;
        var high = maxAppliableIndex();

        if (low >= high)
            return List.of();

        var maxCurrentAllowedSize = config.maxApplyingEntriesSize() - applyingEntriesSize;

        if (maxCurrentAllowedSize <= 0)
            throw new IllegalStateException(
                "applying entries size (" + applyingEntriesSize + ") exceeds limit (" + config.maxApplyingEntriesSize() + "), applyingEntriesPaused should have been true");

        return slice(low, high, maxCurrentAllowedSize);
    }

    /* ==================== STATE UPDATES ==================== */

    /**
     * Restores log state from a snapshot. Resets the committed index
     * and replaces unstable state.
     *
     * @param snapshotToRestore the snapshot to restore from
     */
    public void restore(Snapshot snapshotToRestore) {
        committed = snapshotToRestore.index();
        unstableLog.restore(snapshotToRestore);
    }

    /**
     * Returns {@code true} if another log (identified by its last entry)
     * is at least as up-to-date as ours.
     *
     * <p>A log is more up-to-date if it has a higher last term, or the
     * same last term with a higher or equal last index.</p>
     *
     * @param otherTerm  term of the other log's last entry
     * @param otherIndex index of the other log's last entry
     * @return {@code true} if the other log is at least as up-to-date
     * @throws StorageException if the storage read fails
     */
    public boolean isUpToDate(long otherTerm, long otherIndex) throws StorageException {
        var lastIndex = lastIndex();
        var lastTerm = term(lastIndex);
        return otherTerm > lastTerm || (otherTerm == lastTerm && otherIndex >= lastIndex);
    }

    /**
     * Reads entries in the half-open range {@code [low, high)} from both
     * storage and unstable, handling the boundary transparently.
     *
     * @param low     start index (inclusive)
     * @param high    end index (exclusive)
     * @param maxSize maximum total size in bytes
     * @return entries in the range, possibly truncated to fit {@code maxSize}
     */
    public List<Entry> slice(long low, long high, long maxSize) {
        try {
            checkIndexBounds(low, high);
            if (low == high)
                return List.of();

            if (low >= unstableLog.offset()) {
                var entries = unstableLog.slice(low, high);
                removeEntriesOutOfSizeBound(entries, maxSize);
                return entries;
            }

            var lastIndexForStorage = Math.min(high, unstableLog.offset());
            var entriesFromLog = log.entries(low, lastIndexForStorage, maxSize);

            // requested entries are only from LogStorage
            if (high <= unstableLog.offset())
                return entriesFromLog;

            // returned entries are lesser than requested meaning entries has reached the size limits
            // therefore we can't insert further entries from unstable log if present
            if (entriesFromLog.size() < lastIndexForStorage - low)
                return entriesFromLog;

            var size = Entry.calculateSize(entriesFromLog);
            if (size >= maxSize)
                return entriesFromLog;

            var unstableEntries = unstableLog.slice(lastIndexForStorage, high);
            removeEntriesOutOfSizeBound(unstableEntries, maxSize - size, 0);

            var result = new ArrayList<Entry>(unstableEntries.size() + entriesFromLog.size());
            result.addAll(entriesFromLog);
            result.addAll(unstableEntries);

            return result;
        } catch (StorageException e) {
            return List.of();
        }
    }

    /**
     * Advances the committed index. Never decreases.
     *
     * @param commitIndex the new committed index
     * @throws StorageException         if the storage read fails
     * @throws IllegalArgumentException if {@code commitIndex} is beyond
     *                                  the last index
     */
    public void commitTo(long commitIndex) throws StorageException {
        if (committed < commitIndex) {
            if (lastIndex() < commitIndex) {
                throw new IllegalArgumentException("commitIndex(" + commitIndex + ") is beyond lastIndex(" + lastIndex() + ")");
            }
            committed = commitIndex;
        }
    }


    /**
     * Attempts to advance the committed index to {@code index} if the
     * entry at that index matches the given term.
     *
     * @param term  the expected term at {@code index}
     * @param index the candidate commit index
     * @return {@code true} if the committed index was advanced
     * @throws StorageException if the storage read fails
     */
    public boolean tryCommit(long term, long index) throws StorageException {
        if (index > committed && matchTerm(term, index)) {
            commitTo(index);
            return true;
        }
        return false;
    }

    /**
     * Acknowledges that entries up to {@code appliedIndex} have been
     * applied to the state machine. Advances the applied pointer and
     * adjusts backpressure tracking.
     *
     * @param appliedIndex       the highest index applied
     * @param appliedEntriesSize total size of the applied entries in bytes
     * @throws IllegalArgumentException if {@code appliedIndex} is out of
     *                                  the valid range
     */
    public void appliedTo(long appliedIndex, long appliedEntriesSize) {
        if (committed < appliedIndex || appliedIndex < applied) {
            throw new IllegalArgumentException("applied(" + appliedIndex + ") is out of range [prevApplied(" + applied + " ), committed(" + committed + ")]");
        }

        applied = appliedIndex;
        applying = Math.max(applying, applied);

        if (applyingEntriesSize > appliedEntriesSize) {
            applyingEntriesSize -= appliedEntriesSize;
        } else {
            applyingEntriesSize = 0;
        }

        applyingEntriesPaused = applyingEntriesSize >= config.maxApplyingEntriesSize();
    }

    /* ==================== PERSISTENCE ACKNOWLEDGEMENT ==================== */

    /**
     * Acknowledges that entries up to ({@code term}, {@code persistedIndex})
     * have been persisted. Removes them from the unstable log.
     *
     * @param term           the term of the persisted entries
     * @param persistedIndex the last persisted index
     */
    public void stableTo(long term, long persistedIndex) {
        unstableLog.stableTo(term, persistedIndex);
    }

    /**
     * Acknowledges that the snapshot has been persisted to storage.
     *
     * @param index the persisted snapshot's index
     */
    public void stableSnapshotTo(long index) {
        unstableLog.stableSnapshotTo(index);
    }

    /**
     * Marks current unstable entries and snapshot as in-progress.
     * They will not be returned in subsequent output until new entries
     * or a new snapshot arrive.
     */
    public void acceptUnstable() {
        unstableLog.acceptInProgress();
    }

    /**
     * Marks entries as being applied to the state machine. Updates the
     * applying pointer and backpressure tracking.
     *
     * <p>Application is paused in two cases:</p>
     * <ol>
     *   <li>The outstanding applying size equals or exceeds the limit.</li>
     *   <li>The outstanding size is within the limit, but
     *       {@code index < maxAppliableIndex()} - meaning the entries
     *       returned by {@link #nextCommittedEntries()} were truncated
     *       to fit the size limit, so the next entry would push over.</li>
     * </ol>
     *
     * @param index       the highest index being applied
     * @param entriesSize total size of entries being applied in bytes
     * @throws IllegalArgumentException if {@code index} exceeds the
     *                                  committed index
     */
    public void acceptApplying(long index, long entriesSize) {
        if (committed < index)
            throw new IllegalArgumentException("applying(" + index + ") is out of range [prevApplying(" + applying + " ), committed(" + committed + " )]");

        applying = index;
        applyingEntriesSize += entriesSize;

        applyingEntriesPaused = applyingEntriesSize >= config.maxApplyingEntriesSize() || index < maxAppliableIndex();
    }

    /**
     * Returns {@code true} if there are committed entries ready to be
     * applied. Returns {@code false} if application is paused by
     * backpressure or a snapshot is pending.
     *
     * @return {@code true} if entries are available for application
     */
    public boolean hasCommittedEntriesToApply() {
        if (applyingEntriesPaused) {
            return false;
        }

        if (hasUnstableSnapshot() || isSnapshotInProgress()) {
            return false;
        }

        return applying < maxAppliableIndex();

    }

    /**
     * Returns the maximum index that can be applied.
     *
     * <p>With {@link AppliableEntriesPolicy#COMMITTED COMMITTED}, equals
     * the committed index - entries can be applied even before persistence.
     * With {@link AppliableEntriesPolicy#PERSISTED_COMMITTED PERSISTED_COMMITTED},
     * capped at {@code unstable.offset - 1} so only already-persisted entries
     * within the committed range are eligible.</p>
     *
     * @return maximum appliable index
     */
    public long maxAppliableIndex() {
        return switch (config.appliableEntriesPolicy()) {
            case AppliableEntriesPolicy.PERSISTED_COMMITTED -> Math.min(committed, unstableLog.offset() - 1);
            case AppliableEntriesPolicy.COMMITTED -> committed;
        };
    }

    /**
     * Returns a paginated iterator over entries in the half-open range
     * {@code [low, high)}. Each page is capped at {@code pageSize} bytes,
     * avoiding loading the entire range into memory at once.
     *
     * @param low      start index (inclusive)
     * @param high     end index (exclusive)
     * @param pageSize maximum bytes per page
     * @return an iterator over the entries
     * @throws IllegalArgumentException if the range is invalid
     * @throws StorageException         if the bounds check fails
     */
    public Iterator<Entry> iterator(long low, long high, long pageSize) throws IllegalArgumentException, StorageException {
        checkIndexBounds(low, high);
        return new Iterator<>() {
            long cursor = low;
            Iterator<Entry> current = Collections.emptyIterator();
            @Override
            public boolean hasNext() {
                if (cursor > high) {
                    return false;
                }
                if (current.hasNext()) {
                    return true;
                }

                var entries = slice(cursor, high, pageSize);
                if (entries.isEmpty()) {
                    return false;
                }
                cursor += entries.size();
                current = entries.iterator();
                return true;
            }

            @Override
            public Entry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }
        };
    }

    /**
     * Returns all committed entries that have not yet been applied,
     * in the range {@code (applying, committed]}.
     *
     * @return all appliable entries
     */
    public List<Entry> allAppliableEntries() {
        return slice(applying + 1, committed + 1, Long.MAX_VALUE);
    }

    /* ==================== INTERNAL HELPERS ==================== */

    /**
     * Removes entries from the end of the list that exceed
     * {@code maxSize}. Keeps at least one entry.
     *
     * @param entries the list to truncate (modified in place)
     * @param maxSize maximum total size allowed in bytes
     * @return total size of the kept entries
     */
    private long removeEntriesOutOfSizeBound(List<Entry> entries, long maxSize) {
        return removeEntriesOutOfSizeBound(entries, maxSize, 1);
    }

    /**
     * Removes entries from the end of the list that exceed
     * {@code maxSize}. Keeps at least {@code minEntriesToKeep} entries
     * regardless of size.
     *
     * @param entries          the list to truncate (modified in place)
     * @param maxSize          maximum total size allowed in bytes
     * @param minEntriesToKeep minimum entries to keep
     * @return total size of the kept entries
     */
    private long removeEntriesOutOfSizeBound(List<Entry> entries, long maxSize, final int minEntriesToKeep) {
        if (entries.isEmpty())
            return 0;

        var entriesWithinBound = minEntriesToKeep;
        var size = Entry.calculateSize(entries.subList(0, minEntriesToKeep));
        // can have entries up to index where size of current entry won't exceed max allowed entries size
        while (entriesWithinBound < entries.size()) {
            var entrySize = entries.get(entriesWithinBound).size();
            if (size + entrySize > maxSize)
                break;
            size += entrySize;
            entriesWithinBound++;
        }

        while (entries.size() > entriesWithinBound) {
            entries.removeLast();
        }

        return size;
    }

    /**
     * Validates that {@code [low, high)} is within valid log bounds.
     *
     * @param low  start index (inclusive)
     * @param high end index (exclusive)
     * @throws IllegalArgumentException if {@code low > high} or
     *                                  {@code high} is beyond the last index
     * @throws CompactedException       if {@code low} is before the first index
     * @throws StorageException         if the storage read fails
     */
    private void checkIndexBounds(long low, long high) throws IllegalArgumentException, StorageException {
        if (low > high)
            throw new IllegalArgumentException("Invalid Range: low=" + low + " high=" + high);

        var firstIndex = firstIndex();

        if (low < firstIndex)
            throw new CompactedException(low);

        var lastIndex = lastIndex();
        var length = lastIndex - firstIndex + 1;

        if (high > firstIndex + length)
            throw new IllegalArgumentException("Slice [" + low + ", " + high + ") out of bounds [" + firstIndex + ", " + lastIndex + ")");

    }
}
