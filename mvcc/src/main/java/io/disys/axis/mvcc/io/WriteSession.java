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

public class WriteSession implements AutoCloseable {
    private final WriteTxn txn;
    private final KeyTimelineIndex index;
    private final RevisionRecordBuffer buffer;
    private final CommitSeqBound bound;
    private final VersionedStoreConfig config;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    private final long expiryTime;

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
        this.expiryTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.revisionRecordBufferSyncTimeout());
    }

    boolean expired() {
        return buffer.size() >= config.maxRevisionRecordBuffer() ||
                System.nanoTime() >= expiryTime;
    }

    public boolean closeIfExpired() {
        if (expired()) {
            close();
            return true;
        }

        return false;
    }

    @Override
    public void close() {
        txn.put(
                db.meta(),
                db.meta().persistedCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(bound.end()).array());
        txn.commit();
        txn.close();
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < bound.start();
    }

    private boolean future(long commitSeq) {
        return commitSeq > bound.end() + 1;
    }

    public void advance()  {
        buffer.publish();
        bound.advance();
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

    public void put(byte[] key, byte[] val, int ordinal)  {
        var revision = Revision.modify(bound.end() + 1, ordinal);

        var timeline = index.add(key, revision);
        var span = timeline.lastSpan();

        var record = new Record(key, val, span);
        buffer.stage(new RevisionRecord(revision, record));

        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
    }

    public boolean delete(byte[] key, int ordinal) {
        var revision = Revision.modify(bound.end() + 1, ordinal);
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

    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, bound.start(), bound.end()));
    }

    public Optional<Record> get(byte[] key) {
        return index.get(key)
                .map(KeyTimeline::floor)
                .map(r -> get(key, r));
    }

    public ReadResult getAt(byte[] key, long commitSeq) {
        if (compacted(commitSeq)) {
            return new ReadResult.Compacted(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new ReadResult.Future(bound.end(), commitSeq);
        }

        return index.revision(key, commitSeq)
                .map(r -> get(key, r))
                .<ReadResult>map(ReadResult.Present::new)
                .orElseGet(ReadResult.Absent::new);
    }


    public RecordIterator range(byte[] from, byte[] to) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1);
    }

    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, modifiedAtSeqBound);
    }

    public RecordIterator range(byte[] from, byte[] to, long limit) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, limit);
    }

    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return new WriterRecordIterator(this, index.range(from, to), bound.end() + 1, modifiedAtSeqBound, limit);
    }

    private RangeResult rangeOutsideVisibleWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new RangeResult.Compacted(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new RangeResult.Future(bound.end() + 1, commitSeq);
        }

        return null;
    }

    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = rangeOutsideVisibleWindow(commitSeq);
        return result != null ? result : new RangeResult.Range(
                new WriterRecordIterator(this, index.range(from, to), commitSeq)
        );
    }

    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = rangeOutsideVisibleWindow(commitSeq);
        return result != null ? result :  new RangeResult.Range(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, modifiedAtSeqBound)
        );
    }

    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = rangeOutsideVisibleWindow(commitSeq);
        return result != null ? result : new RangeResult.Range(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, limit)
        );
    }

    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = rangeOutsideVisibleWindow(commitSeq);
        return result != null ? result :  new RangeResult.Range(
                new WriterRecordIterator(this, index.range(from, to), commitSeq, modifiedAtSeqBound, limit)
        );
    }

    public KeyIterator keys(byte[] from, byte[] to) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1);
    }

    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, modifiedAtSeqBound);
    }

    public KeyIterator keys(byte[] from, byte[] to, long limit) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, limit);
    }

    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return new WriterKeyIterator(index.range(from, to), bound.end() + 1, modifiedAtSeqBound, limit);
    }

    private KeyRangeResult keysOutsideVisibleWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new KeyRangeResult.Compacted(bound.start(), commitSeq);
        }
        if (future(commitSeq)) {
            return new KeyRangeResult.Future(bound.end() + 1, commitSeq);
        }

        return null;
    }

    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = keysOutsideVisibleWindow(commitSeq);
        return result != null ? result : new KeyRangeResult.Range(
                new WriterKeyIterator(index.range(from, to), commitSeq));
    }

    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = keysOutsideVisibleWindow(commitSeq);
        return result != null ? result : new KeyRangeResult.Range(
                new WriterKeyIterator(index.range(from, to), commitSeq, modifiedAtSeqBound));
    }

    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = keysOutsideVisibleWindow(commitSeq);
        return result != null ? result : new KeyRangeResult.Range(
                new WriterKeyIterator(index.range(from, to), commitSeq, limit));
    }

    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = keysOutsideVisibleWindow(commitSeq);
        return result != null ? result : new KeyRangeResult.Range(
                new WriterKeyIterator(index.range(from, to), commitSeq, modifiedAtSeqBound, limit));
    }
}
