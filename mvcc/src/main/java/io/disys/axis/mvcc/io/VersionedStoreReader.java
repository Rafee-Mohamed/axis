package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.ReadTxn;

import java.nio.ByteBuffer;
import java.util.Optional;

public class VersionedStoreReader implements Reader {

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

    private VersionedStoreReader(
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


    public static VersionedStoreReader create(
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
        return new VersionedStoreReader(
                db,
                index,
                txn,
                buffer.view(firstCommitSeq, lastCommitSeq),
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

    @Override
    public Optional<Record> get(byte[] key) {
        return index.revision(key, firstCommitSeq, lastCommitSeq)
                .map(r -> get(key, r));
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
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.revision(key, firstCommitSeq, commitSeq)
                .map(r -> get(key, r))
        );
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to) {
        return new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, lastCommitSeq);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound) {
        return new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, lastCommitSeq, bound);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, long limit) {
        return new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, lastCommitSeq, limit);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit) {
        return new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, lastCommitSeq, bound, limit);
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, commitSeq)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, commitSeq, bound)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result =  this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, commitSeq, limit)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderRecordIterator(this, index.range(from, to), firstCommitSeq, commitSeq, bound, limit)
        );
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to) {
        return new ReaderKeyIterator(index.range(from, to), firstCommitSeq, lastCommitSeq);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound) {
        return new ReaderKeyIterator(index.range(from, to), firstCommitSeq, lastCommitSeq, bound);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, long limit) {
        return new ReaderKeyIterator(index.range(from, to), firstCommitSeq, lastCommitSeq, limit);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit) {
        return new ReaderKeyIterator(index.range(from, to), firstCommitSeq, lastCommitSeq, bound, limit);
    }
    
    
    @Override
    public SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderKeyIterator(index.range(from, to), firstCommitSeq, commitSeq));
    }

    @Override
    public SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderKeyIterator(index.range(from, to), firstCommitSeq, commitSeq, bound));
    }

    @Override
    public SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderKeyIterator(index.range(from, to), firstCommitSeq, commitSeq, limit));
    }

    @Override
    public SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new ReaderKeyIterator(index.range(from, to), firstCommitSeq, commitSeq, bound, limit));
    }

    @Override
    public void close() {
        txn.close();
    }
}
