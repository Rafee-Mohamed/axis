package consensus.storage;

import consensus.core.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Holds log entries and a snapshot that have not yet been persisted
 * to {@link LogStorage}.
 *
 * <p>Serves two purposes:</p>
 * <ol>
 *   <li>Buffers new entries and snapshots until they are handed to
 *       {@link consensus.engine.RaftOutput} for persistence.</li>
 *   <li>Continues holding them (marked "in progress") until persistence
 *       is confirmed, so {@link RaftLog} always has a complete view of
 *       the log.</li>
 * </ol>
 *
 * <p>{@code entries.get(i)} has logical Raft log index
 * {@code offset + i}.</p>
 */
public class UnstableLog {

    /** Pending snapshot awaiting persistence, or {@code null} if none. */
    private Snapshot snapshot;

    /** Entries not yet persisted to {@link LogStorage}. */
    private List<Entry> entries;

    /**
     * Logical index of {@code entries[0]}.
     * {@code entries.get(i).index() == offset + i}.
     */
    private long offset;

    /**
     * Entries in {@code [offset, persistingUpTo)} are currently being
     * written to storage.
     * Invariant: {@code offset <= persistingUpTo <= offset + entries.size()}.
     */
    private long persistingUpTo;

    /** Whether the snapshot is currently being written to storage. */
    boolean snapshotInProgress;

    /**
     * Creates an unstable log starting after the last persisted index.
     *
     * @param storageLastIndex the last index in {@link LogStorage}
     */
    public UnstableLog(long storageLastIndex) {
        this.entries = new ArrayList<>();
        this.offset = storageLastIndex + 1;
        this.persistingUpTo = this.offset;
        this.snapshot = null;
        this.snapshotInProgress = false;
    }

    /* ==================== GETTERS ==================== */

    /**
     * Returns the logical index where unstable entries begin.
     *
     * @return the offset index
     */
    public long offset() {
        return offset;
    }

    /* ==================== INDEX QUERIES ==================== */

    /**
     * Returns the first available index if a snapshot is pending.
     *
     * <p>When a snapshot exists it will replace storage up to
     * {@code snapshot.index}, so the first available entry is at
     * {@code snapshot.index + 1}.</p>
     *
     * @return {@code snapshot.index + 1} if a snapshot exists, empty otherwise
     */
    public OptionalLong firstIndex() {
        if (snapshot == null) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(snapshot.index() + 1);
    }

    /**
     * Returns the last index in the unstable log.
     *
     * <p>Entries take priority over the snapshot since they represent
     * newer state.</p>
     *
     * @return last entry index, or snapshot index if only a snapshot
     *         exists, or empty if both are absent
     */
    public OptionalLong lastIndex() {
        if (!entries.isEmpty())
            return OptionalLong.of(offset + entries.size() - 1);

        if (snapshot != null)
            return OptionalLong.of(snapshot.index());

        return OptionalLong.empty();
    }

    /**
     * Returns the term at the given index if it exists in the unstable log.
     *
     * <p>Checks the snapshot first (for a matching index), then entries.
     * Used by {@link RaftLog} to avoid storage reads when the entry is
     * still in memory.</p>
     *
     * @param index the log index to query
     * @return the term at that index, or empty if not found
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

    /* ==================== OUTPUT ACCESS ==================== */

    /**
     * Returns entries that are not yet being persisted.
     *
     * <p>Entries already marked in-progress via {@link #acceptInProgress()}
     * are excluded.</p>
     *
     * @return entries pending persistence, or an empty list
     */
    public List<Entry> nextEntriesToPersist() {
        var inProgressCount = (int) (persistingUpTo - offset);

        if (inProgressCount >= entries.size())
            return List.of();

        return entries.subList(inProgressCount, entries.size());
    }

    /**
     * Returns the number of entries not yet marked in-progress.
     *
     * @return count of entries pending persistence
     */
    public long entriesToPersistCount() {
        var inProgressCount = (int) (persistingUpTo - offset);
        return Math.max(0, entries.size() - inProgressCount);
    }

    /**
     * Returns the total number of entries in the unstable log.
     *
     * @return total entry count (including in-progress)
     */
    public long totalEntriesCount() {
        return entries.size();
    }

    /**
     * Returns the snapshot if it is not yet being persisted.
     *
     * @return the pending snapshot, or {@code null} if none or already
     *         in-progress
     */
    public Snapshot nextSnapshot() {
        if (snapshot == null || snapshotInProgress)
            return null;

        return snapshot;
    }

    /* ==================== PROGRESS TRACKING ==================== */

    /**
     * Marks all current entries and the snapshot as in-progress.
     *
     * <p>Once marked, they will not be returned by
     * {@link #nextEntriesToPersist()} or {@link #nextSnapshot()} again
     * until new entries or a new snapshot arrive.</p>
     */
    public void acceptInProgress() {
        if (!entries.isEmpty())
            persistingUpTo = entries.getLast().index() + 1;

        if (snapshot != null)
            snapshotInProgress = true;
    }

    /**
     * Acknowledges that entries up to ({@code term}, {@code index}) have
     * been persisted to storage.
     *
     * <p>Removes acknowledged entries from the unstable log. The
     * {@code term} parameter guards against stale acknowledgements: if
     * the log was replaced (e.g., by a new leader) while persistence was
     * in-flight, the term will not match and the stale ack is ignored.</p>
     *
     * @param term  the term of the last persisted entry
     * @param index the index of the last persisted entry
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
     * Acknowledges that the snapshot has been persisted to storage.
     * Clears the snapshot if the index matches.
     *
     * @param index the persisted snapshot index
     */
    public void stableSnapshotTo(long index) {
        if (snapshot != null && snapshot.index() == index) {
            snapshot = null;
            snapshotInProgress = false;
        }
    }

    /* ==================== MUTATION ==================== */

    /**
     * Appends entries, truncating any conflicting entries first.
     *
     * <p>Three cases are handled:</p>
     * <ol>
     *   <li>Appending at the end - entries are simply added.</li>
     *   <li>Replacing the entire unstable log - offset is reset.</li>
     *   <li>Partial overlap - conflicting suffix is truncated, then
     *       new entries are appended.</li>
     * </ol>
     *
     * @param newEntries entries to append
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
     * Restores state from a snapshot, clearing all entries.
     *
     * @param snapshotToRestore the snapshot to restore from
     */
    public void restore(Snapshot snapshotToRestore) {
        offset = snapshotToRestore.index() + 1;
        this.persistingUpTo = offset;
        entries = new ArrayList<>();
        snapshot = snapshotToRestore;
        snapshotInProgress = false;
    }

    /**
     * Returns a copy of entries in the half-open range {@code [low, high)}.
     *
     * @param low  start index (inclusive)
     * @param high end index (exclusive)
     * @return copy of entries in the range
     * @throws IllegalArgumentException if the range is invalid or out of bounds
     */
    public List<Entry> slice(long low, long high) throws IllegalArgumentException {
        checkIndexBounds(low, high);
        var startIdx = (int) (low - offset);
        var endIdx = (int) (high - offset);
        return new ArrayList<>(entries.subList(startIdx, endIdx));
    }


    /**
     * Validates that {@code [low, high)} is within bounds.
     *
     * @param low  start index (inclusive)
     * @param high end index (exclusive)
     * @throws IllegalArgumentException if the range is invalid or out of bounds
     */
    private void checkIndexBounds(long low, long high) throws IllegalArgumentException {
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

    /**
     * Returns whether the snapshot is currently being persisted.
     *
     * @return {@code true} if snapshot persistence is in progress
     */
    public boolean snapshotInProgress() {
        return snapshotInProgress;
    }
}
