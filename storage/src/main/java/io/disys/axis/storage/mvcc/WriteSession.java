package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.WriteTxn;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    WriteSession(
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

    boolean closeIfExpired() throws IOException {
        if (expired()) {
            close();
            return true;
        }

        return false;
    }

    @Override
    public void close() throws IOException {
        txn.put(
                db.meta(),
                db.meta().persistedCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(bound.end()).array());
        txn.commit();
        txn.close();
    }

    void put(byte[] key, byte[] val, int ordinal) throws IOException {
        var revision = Revision.modify(bound.end() + 1, ordinal);

        var timeline = index.add(key, revision);
        var span = timeline.lastSpan();

        var record = new Record(key, val, span);
        buffer.stage(new RevisionRecord(revision, record));

        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
    }

    boolean delete(byte[] key, int ordinal) throws IOException {
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

    public ReadResult get(byte[] key) {
        var timeline = index.get(key);
        return timeline.flatMap(
                keyTimeline -> txn.get(db.revision(), encoder.encode(keyTimeline.lastRevision()))
                        .map(decoder::decodeRecord))
                .<ReadResult>map(ReadResult.Present::new)
                .orElseGet(ReadResult.Absent::new);
    }

    public ReadResult getAt(byte[] key, long commitSeq) throws IOException {
        return null;
    }

    public CloseableIterator<Record> range(byte[] start, byte[] end) {
        return null;
    }

    public void advance() throws IOException {
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

    public CloseableIterator<Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq) {
        return null;
    }
}
