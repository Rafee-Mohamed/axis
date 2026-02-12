package consensus.storage;

import consensus.algorithm.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Holds log entries and snapshot that have not yet been persisted to Storage.
 *
 * Serves two purposes:
 * 1. Holds new entries/snapshot until they're handed to Ready for persistence
 * 2. Continues holding them (marked "in progress") until persistence is confirmed,
 *    providing RaftLog with a complete view of the log
 *
 * entries.get(i) has logical raft log index: offset + i
 */
public class UnstableLog {

    // Pending snapshot to be persisted (if any)
    private Snapshot snapshot;

    // Entries not yet persisted to Storage
    private List<Entry> entries;

    // Logical index of entries[0]
    // entries.get(i).index() == offset + i
    private long offset;

    // Entries in range [offset, persistingUpTo) are currently being written to Storage
    // Invariant: offset <= persistingUpTo <= offset + entries.size()
    private long persistingUpTo;

    // Whether snapshot is currently being written to Storage
    private boolean snapshotInProgress;

    /**
     * Creates an UnstableLog starting after the last index in Storage.
     */
    public UnstableLog(long storageLastIndex) {
        this.entries = new ArrayList<>();
        this.offset = storageLastIndex + 1;
        this.persistingUpTo = this.offset;
        this.snapshot = null;
        this.snapshotInProgress = false;
    }

    // ==================== Getters ====================

    /**
     * Returns the logical index where unstable entries begin.
     * Used by RaftLog to determine whether to read from Storage or UnstableLog.
     */
    public long offset() {
        return offset;
    }

    // ==================== Index Queries ====================

    /**
     * Returns the first available index if a snapshot is pending.
     * When a snapshot exists, it will replace Storage up to snapshot.index,
     * so the first available entry would be at snapshot.index + 1.
     *
     * @return snapshot.index + 1 if snapshot exists, empty otherwise
     */
    public OptionalLong firstIndex() {
        if (snapshot == null) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(snapshot.index() + 1);
    }

    /**
     * Returns the last index in the unstable log.
     * Entries take priority over snapshot since they represent newer state.
     *
     * @return last entry index if entries exist, snapshot.index if only snapshot exists, empty otherwise
     */
    public OptionalLong lastIndex() {
        if (!entries.isEmpty())
            return OptionalLong.of(offset + entries.size() - 1);

        if (snapshot != null)
            return OptionalLong.of(snapshot.index());

        return OptionalLong.empty();
    }

    /**
     * Returns the term at the given index if it exists in unstable.
     * Checks snapshot first (for matching index), then entries.
     * Used by RaftLog to avoid disk reads when entry is still in memory.
     *
     * @param index the log index to query
     * @return term at index if found, empty otherwise
     */
    public OptionalLong term(long index) {
        // Check if index matches snapshot index
        if (index < offset && snapshot != null && snapshot.index() == index) {
            return OptionalLong.of(snapshot.term());
        }

        var lastIndex = lastIndex();
        // Check if index is within entries range
        if (index < offset || lastIndex.isEmpty() || index > lastIndex.getAsLong()) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(entries.get((int) (index - offset)).term());
    }

    // ==================== Entry and Snapshot Access for Ready ====================

    /**
     * Returns entries that are NOT yet being persisted.
     * These should be included in the next Ready for the application to persist.
     * Entries already marked in-progress (via acceptInProgress) are excluded.
     *
     * @return list of entries pending persistence, empty list if none
     */
    public List<Entry> nextEntriesToPersist() {
        var inProgressCount = (int) (persistingUpTo - offset);

        if (inProgressCount >= entries.size())
            return List.of();

        return entries.subList(inProgressCount, entries.size());
    }

    public long entriesToPersistCount() {
        var inProgressCount = (int) (persistingUpTo - offset);
        return Math.max(0, entries.size() - inProgressCount);
    }

    public long totalEntriesCount() {
        return entries.size();
    }

    /**
     * Returns the snapshot if it's NOT yet being persisted.
     * Returns null if no snapshot exists or if it's already in-progress.
     *
     * @return pending snapshot, or null
     */
    public Snapshot nextSnapshot() {
        if (snapshot == null || snapshotInProgress)
            return null;

        return snapshot;
    }

    // ==================== Progress Tracking ====================

    /**
     * Marks all current entries and snapshot as "in progress" (being persisted).
     * Called when Ready is generated - these won't be returned by nextEntries/nextSnapshot again.
     */
    public void acceptInProgress() {
        if (!entries.isEmpty())
            persistingUpTo = entries.getLast().index() + 1;

        if (snapshot != null)
            snapshotInProgress = true;
    }

    /**
     * Acknowledges that entries up to (index, term) have been persisted to Storage.
     * Removes acknowledged entries from unstable since they now exist in Storage.
     *
     * The term parameter guards against stale acknowledgements: if the log was
     * replaced (e.g., by a new leader) while persistence was in-flight, the term
     * won't match and the stale ack is safely ignored.
     *
     * @param term the term of the last persisted entry
     * @param index the last persisted entry index
     */
    public void stableTo(long term, long index) {
        var entryTerm = term(index);

        // entry term not found in unstable log
        if (entryTerm.isEmpty())
            return;

        // Index is the snapshot index, not an entry
        if (index < offset)
            return;

        // Term mismatch - log was replaced while persisting
        if (entryTerm.getAsLong() != term)
            return;

        // Remove entries [offset, index] from unstable log
        var stableEntriesCount = (int) (index - offset + 1);
        entries = new ArrayList<>(entries.subList(stableEntriesCount, entries.size()));
        offset = index + 1;
        persistingUpTo = Math.max(persistingUpTo, offset);
    }

    /**
     * Acknowledges that the snapshot has been persisted to Storage.
     * Clears the snapshot from unstable if the index matches.
     *
     * @param index the persisted snapshot index
     */
    public void stableSnapshotTo(long index) {
        if (snapshot != null && snapshot.index() == index) {
            snapshot = null;
            snapshotInProgress = false;
        }
    }

    // ==================== Mutation ====================

    /**
     * Appends entries, truncating any conflicting entries first.
     * Called when receiving entries from leader or proposing locally.
     */
    public void append(List<Entry> newEntries) {
        if (newEntries.isEmpty())
            return;

        var firstNewEntryIndex = newEntries.getFirst().index();

        if (firstNewEntryIndex == offset + entries.size()) {
            // Case 1: appending at end
            entries.addAll(newEntries);
        } else if (firstNewEntryIndex <= offset) {
            // Case 2: replacing entire unstable
            entries = new ArrayList<>(newEntries);
            offset = firstNewEntryIndex;
            persistingUpTo = offset;
        } else {
            // Case 3: truncate conflicting + append
            var nonConflictedEntries = (int) (firstNewEntryIndex - offset);

            while (entries.size() > nonConflictedEntries) {
                entries.removeLast();
            }

            entries.addAll(newEntries);
            persistingUpTo = Math.min(persistingUpTo, firstNewEntryIndex);
        }
    }


    /**
     * Restores state from a snapshot (typically received from leader).
     * Clears all entries and sets the snapshot.
     */
    public void restore(Snapshot snapshotToRestore) {
        offset = snapshotToRestore.index() + 1;
        this.persistingUpTo = offset;
        entries = new ArrayList<>();
        snapshot = snapshotToRestore;
        snapshotInProgress = false;
    }

    /**
     * Returns a copy of entries in the range [low, high).
     * Used by RaftLog when reading entries that span both Storage and UnstableLog.
     *
     * @param low start index (inclusive)
     * @param high end index (exclusive)
     * @return copy of entries in range
     * @throws IllegalArgumentException if range is invalid or out of bounds
     */
    public List<Entry> slice(long low, long high) throws IllegalArgumentException {
        checkIndexBounds(low, high);
        var startIdx = (int) (low - offset);
        var endIdx = (int) (high - offset);
        return new ArrayList<>(entries.subList(startIdx, endIdx));
    }


    private void checkIndexBounds(long low, long high) throws IllegalArgumentException{
        if (low > high)
            throw new IllegalArgumentException("Invalid Range: low=" + low + " high=" + high);

        var maxIndex = offset + entries.size();

        if (low < offset || high > maxIndex)
            throw new IllegalArgumentException("Slice [" + low + ", " + high + ") out of bounds [" + offset + ", " + maxIndex + ")");
    }

    @Override
    public String toString() {
        return "UnstableLog{" +
                "offset=" + offset +
                ", persistingUpTo=" + persistingUpTo +
                ", entries=" + entries.size() +
                ", snapshot=" + (snapshot != null ? snapshot.index() : "none") +
                ", snapshotInProgress=" + snapshotInProgress +
                '}';
    }
}
