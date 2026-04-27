package io.disys.axis.mvcc.io;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.backend.WriteTxn;
import io.disys.axis.mvcc.codec.CodecConstants;
import io.disys.axis.mvcc.codec.RecordDecoder;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.Db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

/**
 * Incrementally deletes obsolete revision records from the backend store in fixed-size batches.
 * Each {@link #compact} call walks {@code [compactedRevision, visibleRevision)}, skips retained
 * floor revisions, deletes up to {@code batchSize} records, and advances the cursor. When the
 * range is exhausted, {@code done} is set and no further deletions occur.
 */
public class BatchCompactor {
    /** Maximum number of records deleted per {@link #compact} call. */
    private final int batchSize;
    /** Cursor - the last deleted revision key; the next batch starts at its lexicographic successor. */
    private byte[] compactedRevision;
    /** Exclusive upper boundary — the first revision at the compaction commitSeq (ordinal 0). */
    private final byte[] visibleRevision;
    private final Db db;
    /** Floor revisions that must survive compaction to answer queries at the compaction point. */
    private final Set<Revision> retained;
    private final RecordDecoder decoder;
    /** True when the full compaction range has been processed. */
    private boolean done;

    public BatchCompactor(
            Db db,
            int batchSize,
            byte[] compactedRevision,
            byte[] visibleRevision,
            Set<Revision> retained,
            RecordDecoder decoder,
            boolean done
    ) {
        this.batchSize = batchSize;
        this.compactedRevision = compactedRevision;
        this.visibleRevision = visibleRevision;
        this.db = db;
        this.retained = retained;
        this.decoder = decoder;
        this.done = done;
    }


    /**
     * Reads the compaction cursor and boundary from meta and returns a fresh compactor.
     *
     * @param handle    read handle to load persisted cursor and boundary from meta
     * @param db        database handles
     * @param batchSize maximum records to delete per {@link #compact} call
     * @param retained  floor revisions that must survive compaction
     * @param decoder   decodes revision keys for retained-set lookup
     * @return a new compactor ready to resume from the last persisted cursor
     */
    public static BatchCompactor create(
            ReadHandle handle,
            Db db,
            int batchSize,
            Set<Revision> retained,
            RecordDecoder decoder
    ) {
        var compactedRevision = handle.get(db.meta(), db.meta().compactedRevisionKey())
                .orElse(CodecConstants.START_REVISION);

        var visibleRevision = handle.get(db.meta(), db.meta().firstCommitSeqKey())
                .map(seq -> ByteBuffer.allocate(CodecConstants.REVISION_SIZE)
                        .put(seq)
                        .putInt(0)
                        .array())
                .orElse(CodecConstants.START_REVISION);
        return new BatchCompactor(
                db,
                batchSize,
                compactedRevision,
                visibleRevision,
                retained,
                decoder,
                false
        );
    }

    /** @return a no-op compactor already marked done; used when no compaction is pending */
    public static BatchCompactor completed() {
        return new BatchCompactor(
                null,
                0,
                null,
                null,
                null,
                null,
                true
        );
    }


    /** @return true if the full compaction range has been processed */
    public boolean done() {
        return done;
    }

    /**
     * Deletes up to {@code batchSize} non-retained records in {@code [compactedRevision, visibleRevision)}
     * and advances the cursor. Sets {@code done} when the range is exhausted.
     *
     * @param txn the write transaction to delete through
     */
    void compact(WriteTxn txn) {
        if (done) {
            return;
        }
        var revisions = new ArrayList<byte[]>();

        // Range is half-open [compactedRevision, visibleRevision): inclusive start, exclusive end.
        // compactedRevision is the last deleted key; that key no longer exists on the next commit, so the
        // iterator’s first hit is the lexicographic successor - same effect as an exclusive lower bound for
        // remaining revision keys without encoding a successor key as the cursor.
        try (var it = txn.range(db.revision(), compactedRevision, visibleRevision)) {
            while (revisions.size() < batchSize && it.hasNext()) {
                var revision = it.next().key();
                if (!retained.contains(decoder.decodeRevision(revision))) {
                    revisions.add(revision);
                }
            }
        }

        done = revisions.size() < batchSize;
        if (done) {
            // range exhausted - persist visibleRevision's commitSeq
            // (the requested compaction boundary) as fully completed
            txn.put(
                    db.meta(),
                    db.meta().completedCompactionCommitSeqKey(),
                    Arrays.copyOf(visibleRevision, 8)
            );
            return;
        }

        for (var revision: revisions) {
            txn.delete(db.revision(), revision);
        }

        txn.put(db.meta(), db.meta().compactedRevisionKey(), revisions.getLast());
        compactedRevision = revisions.getLast();
    }

}
