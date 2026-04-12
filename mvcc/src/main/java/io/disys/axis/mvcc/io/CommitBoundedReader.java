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

    private final KeyTimelineIndex index;
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
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer.View buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long firstCommitSeq,
            long lastCommitSeq
    ) {
        this.db = db;
        this.index = index;
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
            KeyTimelineIndex index,
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
                index,
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

    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, firstCommitSeq, lastCommitSeq));
    }

    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<T>(firstCommitSeq, commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<T>(lastCommitSeq, commitSeq);
        }

        return null;
    }

    @Override
    public Optional<Record> get(byte[] key) {
        return index.pinnedRevisionAt(key, lastCommitSeq)
                .map(r -> get(key, r));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRevisionAt(key, commitSeq)
                        .map(r -> get(key, r))
        );
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, long limit) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .limit(limit)
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .limit(limit)
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .limit(limit)
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, long limit) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .limit(limit)
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return index.pinnedRangeAt(from, to, lastCommitSeq)
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .limit(limit)
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .limit(limit)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .limit(limit)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public long count(byte[] from, byte[] to) {
        return index.pinnedCountAt(from, to, lastCommitSeq).count();
    }

    @Override
    public long count(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.pinnedCountAt(from, to, lastCommitSeq).filter(modifiedAtSeqBound).count();
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedCountAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedCountAt(from, to, commitSeq).filter(modifiedAtSeqBound).count()
        );
    }

    @Override
    public ReadHandle handle() {
        return txn;
    }


    @Override
    public void close() {
        txn.close();
    }
}
