package io.disys.axis.consensus.log;

import io.disys.axis.wal.api.RecordTooLargeException;
import io.disys.axis.wal.api.Wal;
import io.disys.axis.wal.api.WalClosedException;
import io.disys.axis.wal.api.WalConfig;
import io.disys.axis.consensus.transport.codec.EntryCodec;
import io.disys.jaft.engine.PersistentState;
import io.disys.jaft.node.task.PersistTask;
import io.disys.jaft.storage.*;
import io.disys.jaft.core.Snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * WAL-backed Raft log for the SEQUENTIAL execution model.
 *
 * <p>Exposes {@link LogStorage} (read-only) to the Raft engine. Persistence
 * is driven by the apply loop via {@link #persist}, which writes to the WAL
 * first and then applies the update atomically to the in-memory log through
 * {@link ReadWriteInMemoryLogStorage}.</p>
 *
 * <p>The WAL write requires no lock - there is exactly one writer (the apply
 * loop) in the sequential model. Locking is encapsulated entirely inside
 * {@link ReadWriteInMemoryLogStorage}.</p>
 *
 * <p>Use {@link #open} to recover from an existing WAL or start fresh.</p>
 */
public final class SequentialRaftLog implements LogStorage {

    private static final Logger log = LoggerFactory.getLogger(SequentialRaftLog.class);

    private final Wal wal;
    private final ReadWriteInMemoryLogStorage mem;

    private SequentialRaftLog(Wal wal, ReadWriteInMemoryLogStorage mem) {
        this.wal = wal;
        this.mem = mem;
    }

    // ===================== Factory / Recovery =================================

    /**
     * Opens (or creates) a {@link SequentialRaftLog} at the given WAL directory.
     *
     * <p>Replays all WAL records to reconstruct hard state, committed index,
     * snapshot boundary, and log entries. On first boot the WAL is empty and
     * an empty log is returned.</p>
     *
     * @param config WAL configuration (directory, segment size, etc.)
     * @return a fully recovered {@link SequentialRaftLog} ready for use
     * @throws IOException if the WAL directory cannot be read or written
     */
    public static SequentialRaftLog open(WalConfig config) throws IOException {
        var recovery = Wal.recover(config);

        var mem = new InMemoryLogStorage();
        var persistentState = new PersistentState();
        long committedIndex = 0;

        var view = recovery.next();
        while (view != null) {
            ByteBuffer buf;
            while ((buf = view.next()) != null) {
                var record = WalRecordCodec.decode(buf);
                switch (record.getTypeCase()) {
                    case HARD_STATE ->
                            persistentState = WalRecordCodec.decodePersistentState(record.getHardState());

                    case CHECKPOINT ->
                            committedIndex = record.getCheckpoint().getCommittedIndex();

                    case SNAPSHOT -> {
                        var snapshot = WalRecordCodec.decodeSnapshot(record.getSnapshot());
                        try {
                            mem.applySnapshot(snapshot);
                        } catch (SnapshotOutOfDateException e) {
                            log.warn("Skipping out-of-date snapshot at index {} during recovery",
                                    record.getSnapshot().getIndex());
                        }
                    }

                    case ENTRY -> {
                        try {
                            mem.append(List.of(EntryCodec.decode(record.getEntry())));
                        } catch (StorageException e) {
                            throw new IOException("Failed to replay entry during WAL recovery", e);
                        }
                    }

                    case TYPE_NOT_SET ->
                            log.warn("Skipping WAL record with no type set during recovery");
                }
            }
            view.close();
            view = recovery.next();
        }

        mem.setPersistentState(persistentState);
        var wal = recovery.finish();

        return new SequentialRaftLog(wal, new ReadWriteInMemoryLogStorage(mem, committedIndex));
    }

    // ===================== LogStorage (delegates to mem) ======================

    @Override
    public InitialState initialState() throws StorageException {
        return mem.initialState();
    }

    @Override
    public List<Entry> entries(long low, long high, long maxSize) throws StorageException {
        return mem.entries(low, high, maxSize);
    }

    @Override
    public long term(long index) throws StorageException {
        return mem.term(index);
    }

    @Override
    public long firstIndex() {
        return mem.firstIndex();
    }

    @Override
    public long lastIndex() {
        return mem.lastIndex();
    }

    @Override
    public Snapshot snapshot() throws StorageException {
        return mem.snapshot();
    }

    // ===================== Committed index ====================================

    /**
     * Returns the highest committed log index as of the last persisted checkpoint.
     *
     * <p>Used after recovery to determine where the apply loop should resume.</p>
     */
    public long committedIndex() {
        return mem.committedIndex();
    }

    // ===================== Persist (apply loop) ================================

    /**
     * Persists a {@link PersistTask} durably and updates the in-memory log.
     *
     * <p>WAL write happens first with no lock held. Once durable,
     * {@link ReadWriteInMemoryLogStorage#persist} applies all updates atomically
     * under a single write lock. {@link PersistTask#complete()} is called last,
     * delivering responses back to the node inbox.</p>
     *
     * @param task the task produced by the node event loop
     * @throws IOException if the WAL write or in-memory update fails
     */
    public void persist(PersistTask task) throws IOException {
        var records = new ArrayList<ByteBuffer>();

        task.persistentState().ifPresent(ps -> records.add(WalRecordCodec.encodeHardState(ps)));
        task.checkpointState().ifPresent(cs -> records.add(WalRecordCodec.encodeCheckpoint(cs)));
        for (var entry : task.entriesToPersist()) {
            records.add(WalRecordCodec.encodeEntry(entry));
        }
        task.snapshot().ifPresent(s -> records.add(WalRecordCodec.encodeSnapshot(s)));

        if (!records.isEmpty()) {
            try {
                wal.append(records);
            } catch (RecordTooLargeException e) {
                throw new IOException("WAL record exceeds configured max size - check WalConfig.maxRecordSize", e);
            } catch (WalClosedException e) {
                throw new IOException("WAL is closed", e);
            }
        }

        try {
            mem.persist(task);
        } catch (StorageException e) {
            throw new IOException("Failed to apply persist task to in-memory log", e);
        }
    }

    // ===================== Compact (apply loop) ================================

    /**
     * Compacts the log up to {@code index} (inclusive).
     *
     * <p>Truncates WAL segments before {@code index}, then discards the
     * corresponding in-memory entries.</p>
     *
     * @param index the highest index to compact
     * @throws IOException               if the WAL truncation fails
     * @throws CompactedException        if {@code index} is already compacted
     * @throws EntryUnavailableException if {@code index} is beyond the last entry
     */
    public void compact(long index) throws IOException, CompactedException, EntryUnavailableException {
        wal.truncatePrefix(index);
        mem.compact(index);
    }
}
