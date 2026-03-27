package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.ReadTxn;

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


     static VersionedStoreReader create(
            VersionedStore.Db db,
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            StoreState state
    ) {
        var firstCommitSeq = getCommitSeq(txn, db.meta(), db.meta().firstCommitSeqKey());
        var persistedCommitSeq = getCommitSeq(txn, db.meta(), db.meta().persistedCommitSeqKey());
        var lastCommitSeq = state.lastVisibleCommitSeq();
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



    @Override
    public Optional<Record> get(byte[] key) throws IOException {
        var currentSpan = index.get(key).flatMap(KeyTimeline::currentSpan);
        if (currentSpan.isEmpty()) {
            return Optional.empty();
        }
        var latestRevision = currentSpan.get().revision();
        var revision = encoder.encode(latestRevision);

        var revisionRecord = buffer.get(latestRevision);
        return revisionRecord
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), revision)
                .map(decoder::decodeRecord));

    }

    @Override
    public CloseableIterator<KeyVal> range(byte[] key, byte[] val) {
        return null;
    }
}
