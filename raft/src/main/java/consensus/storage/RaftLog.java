package consensus.storage;

import consensus.algorithm.Snapshot;
import consensus.config.RaftConfig;
import consensus.config.RaftLogConfig;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * RaftLog provides a unified view of the Raft log by combining:
 * - LogStorage: persisted entries (on disk/WAL)
 * - UnstableLog: in-memory entries not yet persisted
 *
 * <h2>Log Structure with All Pointers</h2>
 * <pre>
 *  [Snapshot]
 *      │
 *      ▼
 * Index:   0     1     2     3     4     5     6     7     8     9    10    11    12
 *       ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
 *       │snap │  A  │  B  │  C  │  D  │  E  │  F  │  G  │  H  │  I  │  J  │  K  │  L  │
 *       └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
 *         ↑      ↑           ↑           ↑           ↑     ↑                 ↑       ↑
 *      snapshot  │        applied     applying   committed │          persistingUpTo │
 *       .index   │                                         │                         │
 *            firstIndex                                  offset                  lastIndex
 *
 *       │◄─────────────────── LogStorage ──────────────►│◄────── Un stableLog ───────►│
 *                           (persisted)                            (in-memory)
 * </pre>
 *
 * <h2>Entry Ranges</h2>
 * <pre>
 *   Range                       Description
 *   ─────────────────────────────────────────────────────────────────────────────────
 *   [0, firstIndex)             Compacted into snapshot, no longer available
 *   [firstIndex, applied]       Applied to state machine
 *   (applied, applying]         Sent to application, being applied (in-flight)
 *   (applying, committed]       Committed (safe to apply), waiting in queue
 *   (committed, offset)         Persisted but not yet committed (waiting for quorum)
 *   [offset, persistingUpTo)    In UnstableLog, currently being persisted (in-progress)
 *   [persistingUpTo, lastIndex] In UnstableLog, not yet sent for persistence
 * </pre>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>applied ≤ applying ≤ committed ≤ lastIndex</li>
 *   <li>firstIndex = snapshot.index + 1</li>
 *   <li>offset ≤ persistingUpTo ≤ lastIndex + 1</li>
 *   <li>committed never decreases</li>
 *   <li>applied never decreases</li>
 * </ul>
 *
 * <h2>Entry Lifecycle</h2>
 * <ol>
 *   <li>Entry arrives → goes to UnstableLog (after persistingUpTo)</li>
 *   <li>Ready generated → entry included for persistence, persistingUpTo advances</li>
 *   <li>Application persists → stableTo() called, entry removed from UnstableLog, offset advances</li>
 *   <li>Quorum reached → committed advances</li>
 *   <li>Ready generated → entry included in committedEntries, applying advances</li>
 *   <li>Application applies to state machine → appliedTo() called, applied advances</li>
 *   <li>Snapshot taken → entries compacted, firstIndex advances</li>
 * </ol>
 */
public class RaftLog {
    // Persisted log storage (read-only interface)
    private final LogStorage log;

    // In-memory entries not yet persisted
    private final UnstableLog unstableLog;

    // Highest index known to be replicated to a quorum of nodes
    private long committed;

    // Highest index that has been sent to application for applying
    // Invariant: applied <= applying <= committed
    private long applying;

    // Highest index that has been applied to state machine
    // Invariant: applied <= committed
    private long applied;

    // Maximum allowed size of entries being applied (backpressure mechanism)
    private final RaftLogConfig config;

    // Current size of entries being applied
    private long applyingEntriesSize;

    // True when entry application is paused due to size limit
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

    // ==================== Getters ====================

    public long committed() { return committed; }

    public long applied() { return applied; }

    // ==================== Index Queries ====================

    /**
     * Returns the first available log index.
     * If an unstable snapshot exists, returns snapshot.index + 1.
     * Otherwise returns the first index from storage.
     */
    public long firstIndex() throws StorageException {
        return unstableLog.firstIndex().orElse(log.firstIndex());
    }

    /**
     * Returns the last log index.
     * Checks unstable entries first, then falls back to storage.
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
     * Returns the (term, index) of the last entry in the log.
     * Used for log comparison in RequestVote.
     */
    public Entry.Id lastEntryId() throws StorageException {
        var lastIndex = lastIndex();
        return new Entry.Id(term(lastIndex), lastIndex);
    }

    // ==================== Log Matching ====================

    /**
     * Returns true if the log contains an entry at the given index with the given term.
     * Used by followers to verify log consistency before accepting AppendEntries.
     *
     * @param term the expected term
     * @param index the log index to check
     * @return true if entry exists and term matches, false otherwise
     */
    public boolean matchTerm(long term, long index) {
        try {
            return term(index) == term;
        } catch(StorageException e) {
            return false;
        }
    }

    /**
     * Returns entries starting from the given index, up to maxSize bytes.
     * Used by leader to send entries to followers.
     *
     * @param index starting index
     * @param maxSize maximum total size in bytes
     * @return list of entries (may be empty if index > lastIndex)
     */
    public List<Entry> entries(long index, long maxSize) throws StorageException {
        var lastIndex = lastIndex();
        if (index > lastIndex)
            return List.of();

        return slice(index, lastIndex + 1, maxSize);
    }

    /**
     * Returns all entries in the log. Primarily for debugging/testing.
     */
    public List<Entry> allEntries() throws StorageException {
        return entries(firstIndex(), Long.MAX_VALUE);
    }

    /**
     * Attempts to append entries from leader, verifying log consistency first.
     * This is the core method for followers handling AppendEntries.
     *
     * <p>Flow:
     * <ol>
     *   <li>Check if we have entry at prevIndex with prevTerm (log consistency)</li>
     *   <li>If not, return empty (reject - leader will retry with earlier entries)</li>
     *   <li>Find first conflicting entry (same index, different term)</li>
     *   <li>Truncate from conflict point and append new entries</li>
     *   <li>Update committed index to min(leaderCommit, newLastIndex)</li>
     * </ol>
     *
     * @param prevTerm term of entry at prevIndex
     * @param prevIndex index of entry immediately preceding new entries
     * @param entries new entries to append (may be empty for heartbeat)
     * @param leaderCommitIndex leader's committed index
     * @return new lastIndex if successful, empty if log doesn't match
     * @throws IllegalStateException if conflict with already committed entry
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
     * Appends entries to the log (via unstable).
     * Used by leader when proposing new entries.
     *
     * @param entries entries to append
     * @return new lastIndex
     * @throws IllegalArgumentException if entries would overwrite committed entries
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
     * Finds the index of the first entry that conflicts with existing log.
     * An entry conflicts if it has the same index but different term,
     * or if the index doesn't exist in our log.
     *
     * @param entries entries to check against our log
     * @return conflict index if present
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
     * Finds where our log might match another log, given only (term, index) of their entry.
     * Used after AppendEntries rejection to help leader find the right point to retry.
     *
     * Returns the highest index <= given index where our term <= given term.
     * This optimization helps skip over entire terms during log reconciliation.
     *
     * @param term the term from the rejected entry
     * @param index the index from the rejected entry
     * @return (term, index) of best guess for matching point
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

    // ==================== Ready Generation ====================

    /**
     * Returns entries that need to be persisted (from unstable log).
     * These should be included in Ready.entries for the application to persist.
     */
    public List<Entry> nextEntriesToPersist() {
        return unstableLog.nextEntriesToPersist();
    }

    /**
     * Returns true if there are entries waiting to be persisted.
     */
    public boolean hasEntriesToPersist() {
        return unstableLog.entriesToPersistCount() > 0;
    }

    /**
     * Returns true if there are any entries in unstable (either pending or in-progress).
     */
    public boolean hasUnstableEntries() {
        return unstableLog.totalEntriesCount() > 0;
    }

    /**
     * Returns true if there's an unstable snapshot waiting to be applied.
     */
    public boolean hasUnstableSnapshot() {
        return unstableLog.nextSnapshot() != null;
    }

    public boolean isSnapshotInProgress() {
        return unstableLog.snapshotInProgress();
    }

    /**
     * Returns the current snapshot.
     * Checks unstable first (incoming snapshot from leader), then storage.
     */
    public Snapshot snapshot() throws StorageException {
        if (unstableLog.nextSnapshot() != null)
            return unstableLog.nextSnapshot();

        return log.snapshot();
    }

    public Snapshot nextUnstableSnapshot() throws StorageException {
        return unstableLog.nextSnapshot();
    }



    /**
     * Returns committed entries that are ready to be applied to state machine.
     * Range: (applying, maxAppliableIndex].
     *
     * Returns empty if:
     * - Application is paused due to size limit
     * - There's a pending snapshot (must apply snapshot first)
     * - No new entries to apply
     *
     * @return entries to apply, respecting size limits
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
            throw new EntrySizeExceededException(maxCurrentAllowedSize);

        return slice(low, high, maxCurrentAllowedSize);
    }

    // ==================== State Updates ====================

    /**
     * Restores log state from a snapshot (received via InstallSnapshot).
     * Resets committed index and replaces unstable state.
     *
     * @param snapshotToRestore the snapshot to restore from
     */
    public void restore(Snapshot snapshotToRestore) {
        committed = snapshotToRestore.index();
        unstableLog.restore(snapshotToRestore);
    }

    /**
     * Checks if another log (identified by its last entry) is at least as up-to-date as ours.
     * Used in RequestVote to decide whether to grant vote.
     *
     * A log is more up-to-date if:
     * - It has a higher last term, OR
     * - Same last term but higher/equal last index
     *
     * @param otherTerm term of the other log's last entry
     * @param otherIndex index of the other log's last entry
     * @return true if other log is at least as up-to-date
     */
    public boolean isUpToDate(long otherTerm, long otherIndex) throws StorageException {
        var lastIndex = lastIndex();
        var lastTerm = term(lastIndex);
        return otherTerm > lastTerm || (otherTerm == lastTerm && otherIndex >= lastIndex);
    }

    /**
     * Reads entries in range [low, high) from both storage and unstable.
     * Handles the boundary between storage and unstable transparently.
     * Respects maxSize limit for memory efficiency.
     *
     * @param low start index (inclusive)
     * @param high end index (exclusive)
     * @param maxSize maximum total size in bytes
     * @return entries in range, possibly truncated to fit maxSize
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
     * Called when leader sees quorum or follower receives leaderCommit.
     *
     * @param commitIndex new committed index
     * @throws IllegalArgumentException if commitIndex is beyond lastIndex
     */
    public void commitTo(long commitIndex) throws StorageException {
        if (committed < commitIndex) {
            if (lastIndex() < commitIndex) {
                throw new IllegalArgumentException("commitIndex(" + commitIndex + ") is beyond lastIndex(" + lastIndex() + ")");
            }
            committed = commitIndex;
        }
    }


    public boolean tryCommit(long term, long index) throws StorageException {
        if (index > committed && matchTerm(term, index)) {
            commitTo(index);
            return true;
        }
        return false;
    }

    /**
     * Acknowledges that entries up to appliedIndex have been applied to state machine.
     * Advances applied pointer and adjusts backpressure tracking.
     *
     * @param appliedIndex highest index applied
     * @param appliedEntriesSize total size of applied entries
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

    // ==================== Persistence Acknowledgement ====================

    /**
     * Acknowledges that entries up to (term, persistedIndex) have been persisted.
     * Removes them from unstable log since they're now in storage.
     *
     * @param term term of the persisted entries
     * @param persistedIndex last persisted index
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
     * Marks current unstable entries/snapshot as "in progress" (being persisted).
     * Called when Ready is accepted. These won't be returned in subsequent Ready.
     */
    public void acceptUnstable() {
        unstableLog.acceptInProgress();
    }

    /**
     * Marks entries as being applied to state machine.
     * Updates applying pointer and backpressure tracking.
     *
     * @param index highest index being applied
     * @param entriesSize total size of entries being applied
     */
    public void acceptApplying(long index, long entriesSize) {
        if (committed < index)
            throw new IllegalArgumentException("applying(" + index + ") is out of range [prevApplying(" + applying + " ), committed(" + committed + " )]");

        applying = index;
        applyingEntriesSize += entriesSize;

        applyingEntriesPaused = applyingEntriesSize >= config.maxApplyingEntriesSize() || index < maxAppliableIndex();
    }

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
     * the committed index — entries can be applied even before persistence.
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
     * Iterates over entries in range [low, high) in pages.
     * Useful for processing large ranges without loading all into memory.
     *
     * @param low start index (inclusive)
     * @param high end index (exclusive)
     * @param pageSize maximum bytes per page
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

    // ==================== Internal Helpers ====================

    /**
     * Removes entries from the end of the list that exceed maxSize.
     * Keeps at least 1 entry (minEntriesToKeep defaults to 1).
     */
    private long removeEntriesOutOfSizeBound(List<Entry> entries, long maxSize) {
        return removeEntriesOutOfSizeBound(entries, maxSize, 1);
    }

    /**
     * Removes entries from the end of the list that exceed maxSize.
     * Keeps at least minEntriesToKeep entries regardless of size.
     *
     * @param entries list to truncate (modified in place)
     * @param maxSize maximum total size allowed
     * @param minEntriesToKeep minimum entries to keep even if exceeds maxSize
     * @return total size of kept entries
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
     * Validates that the range [low, high) is within valid log bounds.
     *
     * @throws IllegalArgumentException if low > high or high beyond lastIndex
     * @throws CompactedException if low is before firstIndex
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
