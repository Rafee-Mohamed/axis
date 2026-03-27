package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.ReadTxn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class VersionedStoreReader implements Reader {

    private final KeyTimelineIndex index;
    private final ReadTxn txn;
    private final RevisionRecordBuffer buffer;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    private final long firstCommitSeq;
    private final long persistedCommitSeq;
    private final long lastCommitSeq;

    public VersionedStoreReader(
            VersionedStore.Db db,
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long visibleCommitSeq
    ) {
        this.db = db;
        this.index = index;
        this.txn = txn;
        this.buffer = buffer;
        this.encoder = encoder;
        this.decoder = decoder;
        this.firstCommitSeq = getCommitSeq(txn, db.meta(), db.meta().firstCommitSeqKey());
        this.persistedCommitSeq = getCommitSeq(txn, db.meta(), db.meta().persistedCommitSeqKey());
        this.lastCommitSeq = visibleCommitSeq;
    }

    private static long getCommitSeq(ReadTxn txn, VersionedStore.MetaDb db, byte[] key) {
        return txn.get(db, key)
                .map(b -> ByteBuffer.allocate(Long.BYTES).put(b).getLong())
                .orElse(0L);
    }



    @Override
    public Optional<Record> get(byte[] key) throws IOException {
        var currentSpan = index.get(key).flatMap(KeyTimeline::currentSpan);
        if (currentSpan.isEmpty()) {
            return Optional.empty();
        }
        var latestRevision = currentSpan.get().revision();
        var revision = encoder.encodeRevision(latestRevision);

        var revisionRecord = buffer.get(revision);
        return revisionRecord
                .map(encoded -> decoder.decodeRecord(encoded.record()))
                .or(() -> txn.get(db.revision(), revision)
                .map(decoder::decodeRecord));

    }

    @Override
    public CloseableIterator<KeyVal> range(byte[] key, byte[] val) {
        return null;
    }
}
