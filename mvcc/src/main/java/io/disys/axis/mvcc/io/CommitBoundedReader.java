package io.disys.axis.mvcc.io;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.ReadTxn;

import java.nio.ByteBuffer;
import java.util.Optional;

public class CommitBoundedReader implements Reader {

    private final TimelineView view;
    private final TimelineQuery query;
    private final ReadTxn txn;
    // the buffer view is an immutable view into the actual buffer
    // the buffer should be within the bounds of firstCommitSeq and lastCommitSeq
    private final RevisionRecordBuffer.View buffer;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    // bounds [firstCommitSeq, persistedCommitSeq, lastCommitSeq]
    // [firstCommitSeq, persistedCommitSeq] - present in backend
    // (persistedCommitSeq, lastCommitSeq] - at least present in buffer
    // buffer can contain records before persistedCommitSeq but not always.
    // buffer can also contain records beyond written by concurrent writer
    // but reader should be within bounds
    //
    // firstCommitSeq is visible min seq - seq before this are compacted
    // persistedCommitSeq is the max seq that is in backend
    //
    //
    // all the reads are expected to get valid correct result for all
    // revisions within this bound
    // the reads won't access anything out of this bound
    private final long firstCommitSeq;
    private final long lastCommitSeq;

    private CommitBoundedReader(
            VersionedStore.Db db,
            TimelineView view,
            TimelineQuery query,
            ReadTxn txn,
            RevisionRecordBuffer.View buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long firstCommitSeq,
            long lastCommitSeq
    ) {
        this.db = db;
        this.view = view;
        this.query = query;
        this.txn = txn;
        this.buffer = buffer;
        this.encoder = encoder;
        this.decoder = decoder;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
    }

    private static long getCommitSeq(ReadTxn txn, VersionedStore.MetaDb db, byte[] key) {
        return txn.get(db, key)
                .map(b -> ByteBuffer.allocate(Long.BYTES).put(b).flip().getLong())
                .orElse(0L);
    }

    public static CommitBoundedReader create(
            VersionedStore.Db db,
            TimelineView view,
            TimelineQuery query,
            ReadTxn txn,
            RevisionRecordBuffer buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            CommitSeqBound bound
    ) {
        var firstCommitSeq = getCommitSeq(txn, db.meta(), db.meta().firstCommitSeqKey());
        var lastCommitSeq = bound.end();
        return new CommitBoundedReader(
                db,
                view,
                query,
                txn,
                buffer.view(lastCommitSeq),
                encoder,
                decoder,
                firstCommitSeq,
                lastCommitSeq
        );
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < firstCommitSeq;
    }

    private boolean future(long commitSeq) {
        return commitSeq > lastCommitSeq;
    }

    Record get(byte[] key, RevisionData data) {
        return buffer.get(data.revision())
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(data.revision()))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, data.revision(), firstCommitSeq, lastCommitSeq));
    }

    Record get(KeyRevisionData krd) {
        return get(krd.key(), krd.data());
    }

    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(firstCommitSeq, commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(lastCommitSeq, commitSeq);
        }

        return null;
    }

    // ===================== get / getAt =====================

    @Override
    public Optional<Record> get(byte[] key) {
        return view.getAt(key, lastCommitSeq).map(rd -> get(key, rd));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                view.getAt(key, commitSeq).map(rd -> get(key, rd))
        );
    }

    // ===================== range =====================

    @Override
    public Page<Record> range(byte[] from, byte[] to) {
        return query.page(view.rangeAt(from, to, lastCommitSeq), this::get);
    }

    @Override
    public Page<Record> range(byte[] from, byte[] to, RangeOptions options) {
        return query.page(
                view.rangeAt(from, to, lastCommitSeq, options.sortDirection()),
                this::get,
                options
        );
    }

    // ===================== rangeAt =====================

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(view.rangeAt(from, to, commitSeq), this::get)
        );
    }

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(
                        view.rangeAt(from, to, commitSeq, options.sortDirection()),
                        this::get,
                        options
                )
        );
    }

    // ===================== keys =====================

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to) {
        return query.pageKeys(view.rangeAt(from, to, lastCommitSeq));
    }

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options) {
        return query.pageKeys(view.rangeAt(from, to, lastCommitSeq), this::get, options);
    }

    // ===================== keysAt =====================

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(view.rangeAt(from, to, commitSeq))
        );
    }

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(view.rangeAt(from, to, commitSeq), this::get, options)
        );
    }

    // ===================== count =====================

    @Override
    public long count(byte[] from, byte[] to) {
        return view.rangeAt(from, to, lastCommitSeq).count();
    }

    @Override
    public long count(byte[] from, byte[] to, CountOptions options) {
        return query.count(view.rangeAt(from, to, lastCommitSeq), options);
    }

    // ===================== countAt =====================

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                view.rangeAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        if (result != null) return result;
        return new SnapshotResult.Ok<>(query.count(view.rangeAt(from, to, commitSeq), options));
    }

    // ===================== handle / close =====================

    @Override
    public ReadHandle handle() { return txn; }

    @Override
    public void close() { txn.close(); }
}
