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

public class WriteSession implements Writer {
    private final WriteTxn txn;
    private final TimelineTxn tlTxn;
    private final TimelineQuery query;
    private final RevisionRecordBuffer buffer;
    private final CommitSeqBound bound;
    private final VersionedStoreConfig config;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    private final long expiryTime;
    private int ordinal;
    private final BatchCompactor compactor;

    public WriteSession(
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

    boolean expired() {
        return buffer.size() >= config.maxRevisionRecordBuffer() ||
                System.nanoTime() >= expiryTime;
    }

    public boolean commitIfExpired() {
        if (expired()) {
            commit();
            return true;
        }

        return false;
    }

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

    @Override
    public void close() {
        if (ordinal == 0) {
            return;
        }
        ordinal = 0;
        buffer.publish();
        tlTxn.commit();
        bound.advance();
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < bound.start();
    }

    private boolean future(long commitSeq) {
        return commitSeq > bound.next();
    }

    public void compact(long commitSeq) {
        txn.put(
                db.meta(),
                db.meta().firstCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(commitSeq).array());
        bound.compact(commitSeq);
    }

    @Override
    public void put(byte[] key, byte[] val) {
        var revision = Revision.modify(bound.next(), ordinal++);
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
        var revision = Revision.modify(bound.next(), ordinal++);
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

    Record get(RevisionData data) {
        return buffer.get(data.revision())
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(data.revision()))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new IllegalStateException("WriteSession: Record missing for timeline-selected revision: revision=%s, revision bounds=[%d..%d], ordinal=%d"
                                .formatted(data.revision(), bound.start(), bound.end(), ordinal)));
    }

    Record get(KeyRevisionData krd) {
        return get(krd.data());
    }
    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(bound.next(), commitSeq);
        }

        return null;
    }


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
