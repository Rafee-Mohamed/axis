package io.disys.axis.mvcc.io;

import io.disys.axis.backend.ReadTxn;
import io.disys.axis.backend.WriteTxn;
import io.disys.axis.mvcc.codec.CodecConstants;
import io.disys.axis.mvcc.codec.RecordDecoder;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.VersionedStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Set;

public class BatchCompactor {
    private final int batchSize;
    private byte[] compactedRevision;
    private final byte[] visibleRevision;
    private final VersionedStore.Db db;
    private final Set<Revision> retained;
    private final RecordDecoder decoder;
    private boolean done;

    public BatchCompactor(
            VersionedStore.Db db,
            int batchSize,
            byte[] compactedRevision,
            byte[] visibleRevision,
            Set<Revision> retained,
            RecordDecoder decoder
    ) {
        this.batchSize = batchSize;
        this.compactedRevision = compactedRevision;
        this.visibleRevision = visibleRevision;
        this.db = db;
        this.retained = retained;
        this.decoder = decoder;
        this.done = false;
    }


    public static BatchCompactor create(
            ReadTxn txn,
            VersionedStore.Db db,
            int batchSize,
            Set<Revision> retained,
            RecordDecoder decoder
    ) {
        var compactedRevision = txn.get(db.meta(), db.meta().compactedRevisionKey())
                .orElse(CodecConstants.START_REVISION);
        var visibleCommitSeq = txn.get(db.meta(), db.meta().firstCommitSeqKey())
                .map(seq -> ByteBuffer.allocate(CodecConstants.REVISION_SIZE)
                        .put(seq)
                        .putInt(0)
                        .array())
                .orElse(CodecConstants.START_REVISION);
        return new BatchCompactor(
                db,
                batchSize,
                compactedRevision,
                visibleCommitSeq,
                retained,
                decoder
        );
    }

    public boolean done() {
        return done;
    }

    void compact(WriteTxn txn) {
        if (done) {
            return;
        }
        var revisions = new ArrayList<byte[]>();

        // Range is half-open [compactedRevision, visibleRevision): inclusive start, exclusive end.
        // compactedRevision is the last deleted key; that key no longer exists on the next commit, so the
        // iterator’s first hit is the lexicographic successor — same effect as an exclusive lower bound for
        // remaining revision keys without encoding a successor key as the cursor.
        try (var it = txn.range(db.revision(), compactedRevision, visibleRevision)) {
            while (revisions.size() < batchSize && it.hasNext()) {
                var revision = it.next().key();
                if (!retained.contains(decoder.decodeRevision(revision))) {
                    revisions.add(revision);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        done = revisions.size() < batchSize;
        if (revisions.isEmpty()) {
            return;
        }

        for (var revision: revisions) {
            txn.delete(db.revision(), revision);
        }
        txn.put(db.meta(), db.meta().compactedRevisionKey(), revisions.getLast());
        compactedRevision = revisions.getLast();
    }

}
