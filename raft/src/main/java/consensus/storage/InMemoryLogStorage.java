package consensus.storage;

import consensus.core.Snapshot;
import consensus.cluster.membership.JointConfig;
import consensus.cluster.membership.MajorityConfig;
import consensus.cluster.membership.MembershipConfig;
import consensus.cluster.membership.MembershipTransition;
import consensus.engine.PersistentState;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * In-memory {@link LogStorage} implementation for testing and prototyping.
 *
 * <p>All operations are {@code synchronized} so the instance can be
 * shared across threads, but performance is not a concern - this is
 * not intended for production use.</p>
 *
 * <p>The log is backed by an {@link ArrayList} with a
 * {@link Entry.Placeholder} at position zero representing the snapshot
 * boundary. Compaction discards entries up to a given index and
 * replaces them with a new placeholder.</p>
 */
public class InMemoryLogStorage implements LogStorage {

    /** The persisted hard state (term and voted-for). */
    private PersistentState persistentState;

    /** The most recent snapshot. */
    private Snapshot snapshot;

    /** The log entries, starting with a placeholder at the snapshot boundary. */
    private List<Entry> entries;

    /**
     * Creates an empty in-memory log with default initial state.
     */
    public InMemoryLogStorage() {
        persistentState = new PersistentState(0, null);
        var membershipConfig = new MembershipConfig(new JointConfig(new MajorityConfig(Set.of()), new MajorityConfig(Set.of())), Set.of(), Set.of(), MembershipTransition.JOINT_AUTO);
        snapshot = new Snapshot(0, 0, membershipConfig, new byte[0]);
        entries = new ArrayList<>(1024);
        entries.add(new Entry.Placeholder(0, 0));
    }

    /** {@inheritDoc} */
    @Override
    public synchronized InitialState initialState() throws StorageException {
        return new InitialState(persistentState, snapshot.membership());
    }

    /**
     * Returns the index of the placeholder entry (snapshot boundary).
     *
     * @return the offset index
     */
    public synchronized long offset() {
        return entries.getFirst().index();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized List<Entry> entries(long low, long high, long maxSize) throws StorageException {
        var offset = offset();
        if (low <= offset)
            throw new CompactedException(low);

        if (high > lastIndex() + 1)
            throw new EntryUnavailableException(high);

        if (entries.size() == 1)
            throw new EntryUnavailableException(low);

        var startIdx = (int) (low - offset);
        var endIdx = (int) (high - offset);

        var result = new ArrayList<Entry>(endIdx - startIdx);
        long totalSize = 0;

        while (startIdx < endIdx) {
            Entry e = entries.get(startIdx);
            long entrySize = e.size();

            if (!result.isEmpty() && totalSize + entrySize > maxSize) {
                break;
            }
            result.add(e);
            totalSize += entrySize;
            startIdx++;
        }


        return result;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long term(long index) throws StorageException {
        var offset = offset();

        if (index < offset)
            throw new CompactedException(index);

        if (index > lastIndex())
            throw new EntryUnavailableException(index);

        var entryIdx = (int) (index - offset);
        return entries.get(entryIdx).term();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long firstIndex() {
        return entries.getFirst().index() + 1;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized long lastIndex() {
        return entries.getLast().index();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Snapshot snapshot() throws StorageException {
        return snapshot;
    }

    /**
     * Replaces the persisted hard state.
     *
     * @param persistentState the new hard state
     */
    public synchronized void setPersistentState(PersistentState persistentState) {
        this.persistentState = persistentState;
    }

    /**
     * Appends entries to storage, handling compacted and conflicting entries.
     *
     * <p>Called by the application after receiving
     * {@link consensus.engine.RaftOutput#entriesToPersist()}. Handles:</p>
     *
     * <ul>
     *   <li>Entries already compacted - silently skipped</li>
     *   <li>Entries overlapping with existing - existing truncated, new appended</li>
     * </ul>
     *
     * @param newEntries entries to persist
     * @throws StorageException if the append fails
     */
    public synchronized void append(List<Entry> newEntries) throws StorageException {
        if (newEntries.isEmpty())
            return;

        var firstIdx = firstIndex();
        var lastIdx = newEntries.getFirst().index() + newEntries.size() - 1;

        // all entries are already compacted
        if (lastIdx < firstIdx)
            return;

        // truncate entries that are already compacted
        if (firstIdx > newEntries.getFirst().index()) {
            var skip = (int) (firstIdx - newEntries.getFirst().index());
            newEntries = newEntries.subList(skip, newEntries.size());
        }

        var offset = newEntries.getFirst().index() - offset();

        // Truncate conflicting entries - log diverged, newEntries with different term
        while (entries.size() > offset) {
            entries.removeLast();
        }

        entries.addAll(newEntries);
    }


    /**
     * Applies a snapshot, replacing all log entries with the snapshot state.
     *
     * <p>After this call, {@code firstIndex() == nextSnapshot.index() + 1}.</p>
     *
     * @param nextSnapshot the snapshot to apply
     * @throws SnapshotOutOfDateException if the snapshot is older than the current one
     */
    public synchronized void applySnapshot(Snapshot nextSnapshot) throws SnapshotOutOfDateException {
        if (snapshot.index() != 0 && snapshot.index() >= nextSnapshot.index()) {
            throw new SnapshotOutOfDateException(nextSnapshot.index(), snapshot.index());
        }

        snapshot = nextSnapshot;
        entries = new ArrayList<>();
        entries.add(new Entry.Placeholder(snapshot.term(), snapshot.index()));
    }


    /**
     * Discards log entries up to {@code compactIndex} (inclusive).
     *
     * <p>A placeholder is retained at {@code compactIndex} so that
     * {@code term(compactIndex)} remains available for log matching.
     * Only compact up to the applied index - never compact uncommitted
     * entries.</p>
     *
     * @param compactIndex highest index to compact (inclusive)
     * @throws CompactedException        if already compacted past this index
     * @throws EntryUnavailableException if {@code compactIndex > lastIndex()}
     */
    public synchronized void compact(long compactIndex) throws CompactedException, EntryUnavailableException {
        var offset = offset();

        if (compactIndex <= offset)
            throw new CompactedException(compactIndex);

        if (compactIndex > lastIndex())
            throw new EntryUnavailableException(compactIndex);

        var lastCompactEntryIdx = (int) (compactIndex - offset);

        var entriesAfterCompaction = new ArrayList<Entry>(entries.size() - lastCompactEntryIdx + 1);

        entriesAfterCompaction.add(new Entry.Placeholder(entries.get(lastCompactEntryIdx).term(), entries.get(lastCompactEntryIdx).index()));
        entriesAfterCompaction.addAll(entries.subList(lastCompactEntryIdx + 1, entries.size()));

        entries = entriesAfterCompaction;
    }
}
