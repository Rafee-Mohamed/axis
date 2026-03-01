package consensus.storage;

import consensus.algorithm.Snapshot;
import consensus.membership.JointConfig;
import consensus.membership.MajorityConfig;
import consensus.membership.MembershipConfig;
import consensus.membership.MembershipTransition;
import consensus.node.PersistentState;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class InMemoryLogStorage implements LogStorage {
    private PersistentState persistentState;
    private Snapshot snapshot;
    private List<Entry> entries;

    public InMemoryLogStorage() {
        persistentState = new PersistentState(0, 0, null);
        var membershipConfig = new MembershipConfig(new JointConfig(new MajorityConfig(Set.of()), new MajorityConfig(Set.of())), Set.of(), Set.of(), MembershipTransition.JOINT_AUTO);
        snapshot = new Snapshot(0, 0, membershipConfig, new byte[0]);
        entries = new ArrayList<>(1024);
        entries.add(new Entry.Placeholder(0, 0));
    }

    @Override
    public synchronized InitialState initialState() throws StorageException {
        return new InitialState(persistentState, snapshot.membership());
    }

    public synchronized long offset() {
        return entries.getFirst().index();
    }

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

    @Override
    public synchronized long firstIndex() {
        return entries.getFirst().index() + 1;
    }

    @Override
    public synchronized long lastIndex() {
        return entries.getLast().index();
    }

    @Override
    public synchronized Snapshot snapshot() throws StorageException {
        return snapshot;
    }

    public synchronized void setPersistentState(PersistentState persistentState) {
        this.persistentState = persistentState;
    }

    /**
     * Append entries to storage.
     *
     * Called by application after receiving Ready.entries from Raft.
     * These are entries that Raft has accepted but need to be persisted.
     *
     * Handles edge cases:
     * - Entries already compacted (skipped)
     * - Entries overlapping with existing (truncate existing, append new)
     * - Gap between existing and new - diverging log
     *
     * @param newEntries entries from Ready.entriesToPersist
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
     * Apply a snapshot received from leader.
     *
     * Called by application when:
     * - Follower receives InstallSnapshot from leader (too far behind)
     * - Ready.snapshot is non-empty
     *
     * Replaces all log entries with snapshot state.
     * After this, firstIndex = snapshot.index + 1.
     *
     * @param nextSnapshot snapshot from Ready or from leader's InstallSnapshot
     * @throws SnapshotOutOfDateException if snap.index <= current snapshot index
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
     * Compact log entries up to compactIndex.
     *
     * Called by application to reclaim memory/disk after taking a snapshot.
     * Entries before compactIndex are discarded (they're in the snapshot now).
     *
     * Typical flow:
     * 1. Application takes snapshot of state machine at index X
     * 2. Application calls storage.compact(X)
     * 3. Entries [0, X] are discarded, placeholder at X remains
     *
     * IMPORTANT: Only compact up to applied index. Never compact uncommitted entries.
     *
     * @param compactIndex highest index to compact (inclusive)
     * @throws CompactedException if compactIndex already compacted
     * @throws EntryUnavailableException if compactIndex > lastIndex
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
