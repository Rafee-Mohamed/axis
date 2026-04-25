package io.disys.axis.mvcc.store;

import io.disys.axis.backend.ReadTxn;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.dsal.versioned.index.persistent.PersistentBPlusTree;
import io.dsal.versioned.index.persistent.layout.LexigographicPackedByteComparator;
import io.dsal.versioned.index.persistent.layout.PackedByteKeyStorageFactory;

import java.nio.ByteBuffer;

public class VersionedStore {
    private final KeyTimelineIndex index;
    private final TimelineQuery query;
    private final Backend backend;
    private volatile RevisionRecordBuffer buffer;
    private final CommitSeqBound bound;
    private final VersionedStoreConfig config;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final Db db;
    private WriteSession session;
    private BatchCompactor compactor;

    public record Db(Database revision, MetaDb meta) {
    }

    public record MetaDb(
            String name,
            byte[] persistedCommitSeqKey,
            byte[] firstCommitSeqKey,
            byte[] compactedRevisionKey,
            byte[] completedCompactionCommitSeqKey
    ) implements Database {
    }

    private VersionedStore(
            Backend backend,
            VersionedStoreConfig config,
            CommitSeqBound bound,
            Db db,
            KeyTimelineIndex index,
            TimelineQuery query,
            RevisionRecordBuffer buffer,
            WriteSession session,
            BatchCompactor compactor,
            RecordEncoder encoder,
            RecordDecoder decoder
    ) {
        this.backend = backend;
        this.config = config;
        this.index = index;
        this.query = query;
        this.buffer = buffer;
        this.session = session;
        this.compactor = compactor;
        this.bound = bound;
        this.db = db;
        this.encoder = encoder;
        this.decoder = decoder;

    }

    public static VersionedStore restore(Backend backend, VersionedStoreConfig config) {
        var encoder = new RecordEncoder();
        var decoder = new RecordDecoder();
        var index = new KeyTimelineIndex(new PersistentBPlusTree<>(
                config.indexMaxKeys(),
                new PackedByteKeyStorageFactory(new LexigographicPackedByteComparator())
        ));

        var db = getDb(config);
        var txn = backend.beginRead();

        var bound = getBound(txn, db.meta());

        try (var it = txn.range(db.revision())) {
            for (var kv: it) {
                var revision = decoder.decodeRevision(kv.key());
                var record = decoder.decodeRecord(kv.val());

                index.restore(revision, record);
            }
        }

        var buffer = RevisionRecordBuffer.allocate(config.maxRevisionRecordBuffer());
        var completedCompactionCommitSeq = commitSeq(txn, db.meta(), db.meta().completedCompactionCommitSeqKey());

        BatchCompactor compactor;

        if (bound.start() == completedCompactionCommitSeq) {
            compactor = BatchCompactor.completed();
        } else {
            var retained = index.compact(bound.start());
            compactor = BatchCompactor.create(txn, db, config.deleteBatchSize(), retained, decoder);
        }

        txn.close();

        var query = new TimelineQuery();
        var session = new WriteSession(config, db, backend.beginWrite(), index.txn(), query, buffer, bound, encoder, decoder, compactor);

        return new VersionedStore(backend, config, bound, db, index, query, buffer, session, compactor, encoder, decoder);
    }

    private static Db getDb(VersionedStoreConfig config) {
        var metaDb = new MetaDb(
                config.metaDB(),
                config.persistedCommitSeq().getBytes(),
                config.firstCommitSeq().getBytes(),
                config.compactedRevision().getBytes(),
                config.completedCompactionCommitSeq().getBytes()
        );

        return new Db(Database.of(config.versionDB()), metaDb);
    }

    private static CommitSeqBound getBound(ReadTxn txn, MetaDb db) {
        var lastPersistedCommitSeq = commitSeq(txn, db, db.persistedCommitSeqKey());
        var firstVisibleCommitSeq = commitSeq(txn, db, db.firstCommitSeqKey());

        return new CommitSeqBound(firstVisibleCommitSeq, lastPersistedCommitSeq);
    }

    private static long commitSeq(ReadTxn txn, MetaDb db, byte[] key) {
        return txn.get(db,key)
                .map(ByteBuffer::wrap)
                .map(ByteBuffer::getLong)
                .orElse(0L);
    }

    // Multi thread access

    // multiple readers allowed, can called by multiple threads to get readers
    public Reader reader() {
        return CommitBoundedReader.create(db, index.view(), query, backend.beginRead(), buffer, encoder, decoder, bound);
    }

    public void renewBuffer() {
        buffer = RevisionRecordBuffer.allocate(config.maxRevisionRecordBuffer());
    }

    // Only single thread access for writer/compact/sync

    // Behaviour of concurrent threads accessing these are undefined
    // compact removes all revisions existed before given commitSeq
    // that are not present as the point in time view of commitSeq
    public void compact(long commitSeq) {
        if (!compactor.done()) {
            return;
        }

        if (bound.start() >= commitSeq) {
            return;
        }
        // add the compaction point
        session.compact(commitSeq);
        // ----- On creating new Reader, during this interleaving
        // The reader gets the read txn as of now get the snapshot at this point
        // therefore even if the records are deleted along with the session close
        // the reader can see those data as the firstVisibleCommitSeq still not visible to
        // reader. So, while deletion is happening, until the reader lives it can view the
        // data as of this point

        // commit the session flushes any writes and along with commit the compaction point as first visible seq
        session.commit();
        // after this any reader sees the compaction point as the first visible commit seq
        // and reads within the bound even though the upcoming the buffer and index
        // were not taken for read

        // ----- On creating new Reader, during this interleaving
        // the reader may hold the buffer and index as existing now therefore
        // the buffer has the last subset of data that is just flushed into backend
        // Therefore, at the same time the reader can see the same data as in both backend and buffer
        // so the reader should merge or dedup based on persistedCommittedSeq from backend
        // by which the overlapping set of data can be identified
        // Also, the firstVisibleCommitSeq is updated to the compaction point in backend but
        // the reader could see the old buffer and index which has data before the compaction
        // point - once compaction point is committed in backend there is no guarantee on
        // whether those revisions before that point will be in backend, it can be deleted in this point as well
        // or if the reader tries to answer the query based on having data in the index or buffer
        // if those data are before the compaction point, the reads may be inconsistent
        // therefore reader should use the firstVisibleCommitSeq from the backend
        // to bound any results returned, anything read beyond firstVisibleCommitSeq
        // shouldn't return results from buffer or index even if it is present
        // the invariant is that whenever the reader created whatever the bound it sees
        // those data should be present for read, but anything beyond the bound nothing is
        // guaranteed, this is snapshot isolation that the reader has

        // buffer ref swap
        renewBuffer();
        // ----- On creating new Reader, during this interleaving
        // Same as the interleaving before, but the buffer will ge empty but index can
        // have the data - buffer and index compact is not atomic, but that's not an issue
        // if the reader keeps the read within bound. Any read before compaction point
        // should be returned with compacted via published compact commitSeq in backend.
        // The index is concurrently mutated by CoW therefore all readers must view the
        // consistent state of index at any given point
        var tlTxn = index.txn();
        var retained = tlTxn.compact(commitSeq);
        tlTxn.commit();

        // ----- On creating new Reader, during this interleaving
        // all the readers will see a consistent buffer, index and firstVisibleCommitSeq
        // but the actual physical deletion is deferred. But, the only truth is
        // the firstVisibleCommitSeq in backend any read before that seq is not allowed.
        // allowing deletion to happen in async way. but the deletion can happen any time
        // after the compaction point firstVisibleCommitSeq is committed. no guarantee
        // on visibility of revisions before that compaction point

        // from now on new writes on in this session with new buffer and new index
        var txn = backend.beginWrite();
        compactor = BatchCompactor.create(txn, db, config.deleteBatchSize(), retained, decoder);
        session = new WriteSession(config, db, txn, index.txn(), query, buffer, bound, encoder, decoder, compactor);
    }

    public void sync() {
        session.commit();
        renewBuffer();
        session = new WriteSession(config, db, backend.beginWrite(), index.txn(), query, buffer, bound, encoder, decoder, compactor);
    }

    public Writer writer() {
        if (session.commitIfExpired()) {
            // single buffer per session
            // if older readers hold the buffer for read,
            // then mutating the buffer - clear the buffer and use for every session
            // can make the reads can't read from buffer and read txn can't also
            // see the committed result as it sees as of read txn created
            // can result in having a fresh txn but old buffer
            // i.e. concurrent reader creation can be created with read txn that sees latest committed
            // but can take the old buffer before changing it, in that case it is easy
            // for the reader to merge the results, so there is a possibility that
            // records can be present in backend but can hold old buffer
            // so two views of same data, while reading keep this in mind
            renewBuffer();
            session = new WriteSession(config, db, backend.beginWrite(), index.txn(), query, buffer, bound, encoder, decoder, compactor);
        }
        return session;
    }
}
