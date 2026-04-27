package io.disys.axis.mvcc.io;

import io.disys.axis.backend.WriteHandle;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.WriteTxn;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * {@link Writer} implementation that accumulates writes within a single logical commitSeq.
 * Each write advances {@code ordinal} within {@code bound.next()}, staging records into
 * the {@link RevisionRecordBuffer} and {@link TimelineTxn} for immediate read visibility.
 *
 * <p>Writes are buffered to defer backend flushes — flushing on every logical commit would
 * impose the cost of frequent I/O. The {@link RevisionRecordBuffer} bridges logical commits
 * and backend persistence by making staged records readable without waiting for a flush.</p>
 *
 * <p>{@link #close()} is the logical commit: publishes the buffer, commits the timeline
 * transaction, and advances the committed bound. {@link #commit()} additionally flushes
 * the backend transaction and runs one batch of deferred compaction.</p>
 *
 * <p>The session expires when the buffer reaches capacity or the sync timeout elapses.
 * {@link #commitIfExpired()} is called by the store before vending a new writer to keep
 * the buffer and backend within bounds.</p>
 */
public class SessionWriter implements Writer {
    /** Backend write transaction; accumulates puts/deletes until {@link #commit()} flushes them. */
    private final WriteTxn txn;

    /** In-memory timeline transaction; committed on {@link #close()} to make writes queryable. */
    private final TimelineTxn tlTxn;

    private final TimelineQuery query;

    /** Accumulates {@link RevisionRecord}s; published on {@link #close()} to make records visible to readers. */
    private final RevisionRecordBuffer buffer;

    /** Tracks the visible commit window; advanced on {@link #close()}. */
    private final CommitSeqBound bound;

    private final VersionedStoreConfig config;

    private final RecordEncoder encoder;

    private final RecordDecoder decoder;

    private final VersionedStore.Db db;

    /** Wall-clock deadline in nanoseconds; session expires when {@code System.nanoTime()} reaches this. */
    private final long expiryTime;

    /** Position within the current commitSeq; incremented per staged write, reset to 0 on {@link #close()}. */
    private int ordinal;

    /** Runs one deletion batch per {@link #commit()}. */
    private final BatchCompactor compactor;

    public SessionWriter(
            VersionedStoreConfig config,
            VersionedStore.Db db,
            WriteTxn txn,
            TimelineTxn tlTxn,
            TimelineQuery query,
            RevisionRecordBuffer buffer,
            CommitSeqBound bound,
            RecordEncoder encoder,
            RecordDecoder decoder,
            BatchCompactor compactor
    ) {
        this.config = config;
        this.txn = txn;
        this.tlTxn = tlTxn;
        this.query = query;
        this.buffer = buffer;
        this.bound = bound;
        this.encoder = encoder;
        this.decoder = decoder;
        this.db = db;
        this.ordinal = 0;
        this.expiryTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.revisionRecordBufferSyncTimeout());
        this.compactor = compactor;
    }

    /** @return true if the buffer has reached capacity or the sync timeout has elapsed */
    boolean expired() {
        return buffer.size() >= config.maxRevisionRecordBuffer() ||
                System.nanoTime() >= expiryTime;
    }

    /**
     * Commits and returns whether the session was expired. Called by the store before
     * vending a new {@link Writer} to keep the buffer and backend within bounds.
     *
     * @return true if the session was expired and a commit was triggered
     */
    public boolean commitIfExpired() {
        if (expired()) {
            commit();
            return true;
        }

        return false;
    }

    /**
     * Logical commit followed by a backend flush and one compaction batch step.
     * Persists the updated {@code persistedCommitSeq} to meta and closes the transaction.
     */
    public void commit() {
        close();
        txn.put(
                db.meta(),
                db.meta().persistedCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(bound.end()).array());
        compactor.compact(txn);
        txn.commit();
        txn.close();
    }

    /**
     * Logical commit: publishes staged records to the buffer, commits the timeline
     * transaction, and advances the committed bound. Has no effect if no writes were staged.
     */
    @Override
    public void close() {
        if (ordinal == 0) {
            return;
        }
        ordinal = 0;
        // buffer.publish() and tlTxn.commit() must precede bound.advance(): a reader that
        // observes the advanced bound immediately queries the buffer and timeline, so both
        // must already reflect this commitSeq before the bound becomes visible.
        buffer.publish();
        tlTxn.commit();
        bound.advance();
    }

    /**
     * Writes {@code commitSeq} as the new compaction boundary to meta and updates
     * the in-memory bound.
     */
    public void compact(long commitSeq) {
        txn.put(
                db.meta(),
                db.meta().firstCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(commitSeq).array());
        bound.compact(commitSeq);
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < bound.start();
    }

    private boolean future(long commitSeq) {
        return commitSeq > bound.next();
    }

    /**
     * @return Compacted if {@code commitSeq} is below the compaction boundary,
     *         Future if above the writer's current commitSeq, null if within the visible window
     */
    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(bound.next(), commitSeq);
        }

        return null;
    }

    /**
     * Resolves a record by checking the buffer first, then the backend write transaction.
     * Staged writes are in the buffer; previously committed records are in the backend.
     */
    Record get(RevisionData data) {
        return buffer.get(data.revision())
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(data.revision()))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new IllegalStateException("SessionWriter: Record missing for timeline-selected revision: revision=%s, revision bounds=[%d..%d], ordinal=%d"
                                .formatted(data.revision(), bound.start(), bound.end(), ordinal)));
    }

    Record get(KeyRevisionData krd) {
        return get(krd.data());
    }

    @Override
    public void put(byte[] key, byte[] val) {
        var revision = new Revision (bound.next(), ordinal++);
        var span = tlTxn.add(key, revision);

        var record = new Record(key, val, span);
        buffer.stage(new RevisionRecord(revision, record));
        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
    }

    @Override
    public Optional<Record> putAndGet(byte[] key, byte[] val) {
        var record = get(key);
        put(key, val);
        return record;
    }

    @Override
    public boolean delete(byte[] key) {
        var revision = new Revision (bound.next(), ordinal++);
        var span = tlTxn.complete(key, revision);

        if (span.isEmpty()) {
            return false;
        }

        var record = new Record(key, span.get());
        buffer.stage(new RevisionRecord(revision, record));
        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
        return true;
    }

    @Override
    public Optional<Record> deleteAndGet(byte[] key) {
        var existingRecord = get(key);
        existingRecord.ifPresent(_ -> delete(key));
        return existingRecord;
    }

    @Override
    public int deleteRange(byte[] from, byte[] to) {
        var mapper = new BiFunction<byte[], KeyTimeline, Optional<RevisionRecord>>() {
            Revision revision = new Revision(bound.next(), ordinal);;
            @Override
            public Optional<RevisionRecord> apply(byte[] key, KeyTimeline timeline) {
                if (!timeline.tryComplete(revision)) {
                    return Optional.empty();
                }
                var nextRevisionRecord = new RevisionRecord(revision, new Record(key, timeline.lastSpan()));
                revision = revision.next();
                return Optional.of(nextRevisionRecord);
            }
        };

        tlTxn.range(from, to, mapper)
                .forEach(rr -> {
                    buffer.stage(rr);
                    txn.put(db.revision(), encoder.encode(rr.revision()), encoder.encode(rr.record()));
                });

        var deleted = mapper.revision.ordinal() - ordinal;
        ordinal = mapper.revision.ordinal();
        return deleted;
    }

    @Override
    public List<Record> deleteRangeAndGet(byte[] from, byte[] to) {
        var revisionsBeforeDeletion = new ArrayList<RevisionData>();
        var nextCommitSeq = bound.next();
        var mapper = new BiFunction<byte[], KeyTimeline, Optional<RevisionRecord>>() {
            Revision revision = new Revision(nextCommitSeq, ordinal);;
            @Override
            public Optional<RevisionRecord> apply(byte[] key, KeyTimeline timeline) {
                var rd = timeline.getAt(nextCommitSeq);
                rd.ifPresent(revisionsBeforeDeletion::add);
                if (rd.isEmpty() || !timeline.tryComplete(revision)) {
                    return Optional.empty();
                }
                var nextRevisionRecord = new RevisionRecord(revision, new Record(key, timeline.lastSpan()));
                revision = revision.next();
                return Optional.of(nextRevisionRecord);
            }
        };

        tlTxn.range(from, to, mapper)
                .forEach(rr -> {
                    buffer.stage(rr);
                    txn.put(db.revision(), encoder.encode(rr.revision()), encoder.encode(rr.record()));
                });

        ordinal = mapper.revision.ordinal();

        return revisionsBeforeDeletion.stream().map(this::get).toList();
    }

    @Override
    public long revision() {
        return ordinal == 0 ? bound.end() : bound.next();
    }

    @Override
    public WriteHandle handle() {
        return txn;
    }

    // ===================== Reader methods =====================

    @Override
    public Optional<Record> get(byte[] key) {
        return tlTxn.getAt(key, bound.next()).map(this::get);
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                tlTxn.getAt(key, commitSeq).map(this::get)
        );
    }

    // ===================== range =====================

    @Override
    public Page<Record> range(byte[] from, byte[] to) {
        return query.page(tlTxn.rangeAt(from, to, bound.next()), this::get);
    }

    @Override
    public Page<Record> range(byte[] from, byte[] to, RangeOptions options) {
        return query.page(
                tlTxn.rangeAt(from, to, bound.next(), options.sortDirection()),
                this::get,
                options
        );
    }

    // ===================== rangeAt =====================

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(tlTxn.rangeAt(from, to, commitSeq), this::get)
        );
    }

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(
                        tlTxn.rangeAt(from, to, commitSeq, options.sortDirection()),
                        this::get,
                        options
                )
        );
    }

    // ===================== keys =====================

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to) {
        return query.pageKeys(tlTxn.rangeAt(from, to, bound.next()));
    }

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options) {
        return query.pageKeys(tlTxn.rangeAt(from, to, bound.next()), this::get, options);
    }

    // ===================== keysAt =====================

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(tlTxn.rangeAt(from, to, commitSeq))
        );
    }

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(tlTxn.rangeAt(from, to, commitSeq), this::get, options)
        );
    }

    // ===================== count =====================

    @Override
    public long count(byte[] from, byte[] to) {
        return tlTxn.rangeAt(from, to, bound.next()).count();
    }

    @Override
    public long count(byte[] from, byte[] to, CountOptions options) {
        return query.count(tlTxn.rangeAt(from, to, bound.next()), options);
    }

    // ===================== countAt =====================

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                tlTxn.rangeAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.count(tlTxn.rangeAt(from, to, commitSeq), options)
        );
    }
}
