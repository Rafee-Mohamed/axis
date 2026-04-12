package io.disys.axis.consensus.log;

import io.disys.jaft.core.Snapshot;
import io.disys.jaft.engine.PersistentState;
import io.disys.jaft.node.task.PersistTask;
import io.disys.jaft.storage.*;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe {@link LogStorage} backed by {@link InMemoryLogStorage} using a
 * {@link ReentrantReadWriteLock}.
 *
 * <p>Reads (all {@link LogStorage} methods) acquire the read lock, allowing
 * concurrent access from the node event loop. The single write method
 * {@link #persist} acquires the write lock once and applies all updates
 * atomically - hard state, entries, snapshot, and committed index in one
 * critical section, avoiding repeated lock acquisitions per field.</p>
 *
 * <p>Lives in the consensus module because the locking strategy and the
 * {@link PersistTask} coupling are specific to this execution model.</p>
 */
final class ReadWriteInMemoryLogStorage implements LogStorage {

    private final InMemoryLogStorage delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private long committedIndex;

    ReadWriteInMemoryLogStorage(InMemoryLogStorage delegate, long committedIndex) {
        this.delegate = delegate;
        this.committedIndex = committedIndex;
    }

    // ===================== LogStorage (reads - shared lock) ===================

    @Override
    public InitialState initialState() throws StorageException {
        lock.readLock().lock();
        try {
            return delegate.initialState();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> entries(long low, long high, long maxSize) throws StorageException {
        lock.readLock().lock();
        try {
            return delegate.entries(low, high, maxSize);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long term(long index) throws StorageException {
        lock.readLock().lock();
        try {
            return delegate.term(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long firstIndex() {
        lock.readLock().lock();
        try {
            return delegate.firstIndex();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long lastIndex() {
        lock.readLock().lock();
        try {
            return delegate.lastIndex();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Snapshot snapshot() throws StorageException {
        lock.readLock().lock();
        try {
            return delegate.snapshot();
        } finally {
            lock.readLock().unlock();
        }
    }

    // ===================== Committed index (read lock) ========================

    /**
     * Returns the highest committed log index as of the last persisted checkpoint.
     *
     * <p>Used by the apply loop after recovery to determine where to resume
     * applying entries to the state machine.</p>
     */
    long committedIndex() {
        lock.readLock().lock();
        try {
            return committedIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ===================== Persist (single write lock) ========================

    /**
     * Applies all updates from a {@link PersistTask} atomically under a single
     * write lock acquisition.
     *
     * <p>Called by {@link SequentialRaftLog} after the WAL write completes.
     * Hard state, log entries, snapshot, and committed index are all updated
     * in one critical section.</p>
     *
     * @param task the persist task from the node event loop
     * @throws StorageException if any in-memory update fails
     */
    void persist(PersistTask task) throws StorageException {
        lock.writeLock().lock();
        try {
            task.persistentState().ifPresent(delegate::setPersistentState);

            if (!task.entriesToPersist().isEmpty()) {
                delegate.append(task.entriesToPersist());
            }

            if (task.snapshot().isPresent()) {
                delegate.applySnapshot(task.snapshot().get());
            }

            task.checkpointState().ifPresent(cs -> this.committedIndex = cs.commit());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ===================== Compact (write lock) ================================

    /**
     * Discards in-memory entries up to {@code index} (inclusive).
     *
     * <p>Called by {@link SequentialRaftLog} after the WAL prefix has been truncated.</p>
     *
     * @param index the highest index to compact
     * @throws CompactedException        if {@code index} is already compacted
     * @throws EntryUnavailableException if {@code index} is beyond the last entry
     */
    void compact(long index) throws CompactedException, EntryUnavailableException {
        lock.writeLock().lock();
        try {
            delegate.compact(index);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
