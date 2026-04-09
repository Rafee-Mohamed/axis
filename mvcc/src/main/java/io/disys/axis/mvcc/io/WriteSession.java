package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.WriteTxn;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class WriteSession implements Writer {
    private final WriteTxn txn;
    private final KeyTimelineIndex index;
    private final RevisionRecordBuffer buffer;
    private final CommitSeqBound bound;
    private final VersionedStoreConfig config;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    private final long expiryTime;
    private int ordinal;

    public WriteSession(
            VersionedStoreConfig config,
            VersionedStore.Db db,
            WriteTxn txn,
            KeyTimelineIndex index,
            RevisionRecordBuffer buffer,
            CommitSeqBound bound,
            RecordEncoder encoder,
            RecordDecoder decoder) {
        this.config = config;
        this.txn = txn;
        this.index = index;
        this.buffer = buffer;
        this.bound = bound;
        this.encoder = encoder;
        this.decoder = decoder;
        this.db = db;
        this.ordinal = 0;
        this.expiryTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.revisionRecordBufferSyncTimeout());
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
        bound.advance();
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < bound.start();
    }

    private boolean future(long commitSeq) {
        return commitSeq > bound.end() + 1;
    }

    public void compact(long commitSeq) {
        txn.put(
                db.meta(),
                db.meta().firstCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES)
                        .putLong(commitSeq)
                        .array());
        bound.compact(commitSeq);
    }

    @Override
    public void put(byte[] key, byte[] val)  {
        var revision = Revision.modify(bound.end() + 1, ordinal++);

        var timeline = index.add(key, revision);
        var span = timeline.lastSpan();

        var record = new Record(key, val, span);
        buffer.stage(new RevisionRecord(revision, record));

        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
    }

    @Override
    public boolean delete(byte[] key) {
        var revision = Revision.modify(bound.end() + 1, ordinal++);
        var timeline = index.complete(key, revision);

        if (timeline.isEmpty()) {
            return false;
        }

        var span = timeline.get().lastSpan();
        var record = new Record(key, span);

        buffer.stage(new RevisionRecord(revision, record));
        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));

        return true;
    }

    @Override
    public int deleteRange(byte[] from, byte[] to) {
        var revision = new Revision(bound.end() + 1, ordinal);
        for (var it = index.range(from, to); it.hasNext();) {
            var entry = it.next();

            if (!entry.timeline().tryComplete(revision)) {
                continue;
            }

            var record = new Record(entry.key(), entry.timeline().lastSpan());

            buffer.stage(new RevisionRecord(revision, record));
            txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
            revision = revision.next();
        }

        var deleted = revision.ordinal() - ordinal;
        ordinal = revision.ordinal();
        return deleted;
    }


    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(bound.end() + 1, commitSeq);
        }

        return null;
    }


    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, bound.start(), bound.end()));
    }

    @Override
    public Optional<Record> get(byte[] key) {
        return index.get(key)
                .map(KeyTimeline::floor)
                .map(r -> get(key, r));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.revision(key, commitSeq)
                .map(r -> get(key, r))
        );
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, modifiedAtSeqBound);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, long limit) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, limit);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, modifiedAtSeqBound, limit);
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterRecordIterator(this, index.range(from, to), commitSeq)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, modifiedAtSeqBound)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, limit)
        );
    }

    @Override
    public SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<RecordIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, modifiedAtSeqBound, limit)
        );
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, modifiedAtSeqBound);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, long limit) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, limit);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, modifiedAtSeqBound, limit);
    }

    @Override
    public SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterKeyIterator(index.range(from, to), commitSeq));
    }

    @Override
    public SnapshotResult<KeyIterator>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterKeyIterator(index.range(from, to), commitSeq, modifiedAtSeqBound));
    }

    @Override
    public SnapshotResult<KeyIterator>  keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterKeyIterator(index.range(from, to), commitSeq, limit));
    }

    @Override
    public SnapshotResult<KeyIterator>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<KeyIterator>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new WriterKeyIterator(index.range(from, to), commitSeq, modifiedAtSeqBound, limit));
    }

    @Override
    public long count(byte[] from, byte[] to) {
        return index.count(from, to);
    }

    @Override
    public long count(byte[] from, byte[] to, ModifiedAtSeqBound bound) {
        return index.count(from, to, bound);
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(index.count(from, to, commitSeq));
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(index.count(from, to, commitSeq, bound));
    }

}
