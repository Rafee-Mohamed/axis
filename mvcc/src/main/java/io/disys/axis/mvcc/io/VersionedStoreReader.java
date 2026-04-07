package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.CloseableIterator;
import io.disys.axis.backend.ReadTxn;

import java.io.IOException;
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
    private final long persistedCommitSeq;
    private final long lastCommitSeq;

    private VersionedStoreReader(
            VersionedStore.Db db,
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer.View buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long firstCommitSeq,
            long persistedCommitSeq,
            long lastCommitSeq
    ) {
        this.db = db;
        this.index = index;
        this.txn = txn;
        this.buffer = buffer;
        this.encoder = encoder;
        this.decoder = decoder;
        this.firstCommitSeq = firstCommitSeq;
        this.persistedCommitSeq = persistedCommitSeq;
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
        var persistedCommitSeq = getCommitSeq(txn, db.meta(), db.meta().persistedCommitSeqKey());
        var lastCommitSeq = bound.end();
        return new VersionedStoreReader(
                db,
                index,
                txn,
                buffer.view(firstCommitSeq, lastCommitSeq),
                encoder,
                decoder,
                firstCommitSeq,
                persistedCommitSeq,
                lastCommitSeq
        );
    }

    KeyTimelineIndex index() {
        return index;
    }

    RecordEncoder encoder() {
        return encoder;
    }

    RevisionRecordBuffer.View buffer() {
        return buffer;
    }

    RecordDecoder decoder() {
        return decoder;
    }

    ReadTxn txn() {
        return txn;
    }

    VersionedStore.Db db() {
        return db;
    }


    private Optional<KeyTimelineView> timeline(byte[] key, long commitSeq) {
        return index.pin(key, firstCommitSeq, commitSeq);
    }

    private Optional<KeyTimelineView> timeline(byte[] key) {
        return index.pin(key, firstCommitSeq, lastCommitSeq);
    }

    ReadResult get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .<ReadResult>map(ReadResult.Present::new)
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, firstCommitSeq, lastCommitSeq));
    }

    @Override
    public ReadResult get(byte[] key) {
        var revision = timeline(key)
                .flatMap(KeyTimelineView::floor);

        if (revision.isEmpty()) {
            return new ReadResult.Absent();
        }

        return get(key, revision.get());
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < firstCommitSeq;
    }

    private boolean future(long commitSeq) {
        return commitSeq > lastCommitSeq;
    }

    @Override
    public ReadResult getAt(byte[] key, long commitSeq) {
        if (compacted(commitSeq)) {
            return new ReadResult.Compacted(firstCommitSeq, commitSeq);
        }

        if (future(commitSeq)) {
            return new ReadResult.Future(lastCommitSeq, commitSeq);
        }

        var revision = timeline(key)
                .flatMap(tl -> tl.floor(commitSeq));

        if (revision.isEmpty()) {
            return new ReadResult.Absent();
        }

        return get(key, revision.get());
    }

    @Override
    public CloseableIterator<io.disys.axis.mvcc.model.Record> range(byte[] key, byte[] val) {
        return null;
    }

    @Override
    public CloseableIterator<io.disys.axis.mvcc.model.Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq) {
        return null;
    }

    @Override
    public void close() {
        txn.close();
    }
}
