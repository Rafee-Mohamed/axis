package io.disys.axis.mvcc.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.backend.ReadTxn;
import io.disys.axis.mvcc.codec.RecordDecoder;
import io.disys.axis.mvcc.codec.RecordEncoder;
import io.disys.axis.mvcc.io.BatchCompactor;
import io.disys.axis.mvcc.io.CommitBoundedReader;
import io.disys.axis.mvcc.io.RevisionRecordBuffer;
import io.disys.axis.mvcc.io.SessionWriter;
import io.disys.axis.mvcc.model.CommitSeqBound;
import io.disys.axis.mvcc.model.CompactResult;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.timeline.KeyTimelineIndex;
import io.disys.axis.mvcc.timeline.TimelineQuery;
import io.dsal.versioned.index.persistent.PersistentBPlusTree;
import io.dsal.versioned.index.persistent.layout.LexigographicPackedByteComparator;
import io.dsal.versioned.index.persistent.layout.PackedByteKeyStorageFactory;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

/**
 * {@link VersionedStore} implementation backed by a persistent B+ tree timeline index
 * and a write-buffered backend.
 *
 * <p>A single {@link SessionWriter} accumulates logical commits into a
 * {@link RevisionRecordBuffer} until the buffer reaches capacity or the session
 * timeout elapses. Buffered records bridge the gap between logical commit visibility
 * and backend persistence: readers resolve records from the buffer first, falling
 * back to the backend transaction. {@link #sync()} and session expiry flush the
 * backend explicitly.</p>
 *
 * <p>The timeline index ({@link KeyTimelineIndex}) tracks the full revision history
 * of each key. Compaction advances the visibility boundary and prunes the index, but
 * defers physical deletion of backend records to incremental {@link BatchCompactor}
 * batches, keeping write latency flat.</p>
 */
public class TimelineVersionedStore implements VersionedStore {

    /** Persistent B+ tree index mapping each key to its ordered revision history. */
    private final KeyTimelineIndex index;

    /** Executes paged and counted queries over timeline range results. */
    private final TimelineQuery query;

    /** Underlying storage backend; vends read and write transactions. */
    private final Backend backend;

    /**
     * Shared buffer holding revision records staged since the last backend flush.
     * Declared {@code volatile} so buffer swaps in {@link #renewBuffer()} are
     * immediately visible to threads calling {@link #reader()}.
     */
    private volatile RevisionRecordBuffer buffer;

    /** Mutable commit window; tracks the compaction boundary ({@code start}) and the last committed seq ({@code end}). */
    private final CommitSeqBound bound;

    private final TimelineVersionedStoreConfig config;

    /** Encodes {@link Revision} and {@link Record} instances to byte arrays for backend storage. */
    private final RecordEncoder encoder;

    /** Decodes byte arrays from the backend to {@link Revision} and {@link Record} instances. */
    private final RecordDecoder decoder;

    /** Handles to the version and meta backend databases. */
    private final Db db;

    /**
     * Active write session; accumulates logical commits into the buffer until expired,
     * then renewed on the next {@link #writer()} call.
     */
    private SessionWriter session;

    /**
     * Runs one batch of physical record deletion per commit, draining the backlog
     * of backend records below the compaction boundary.
     */
    private BatchCompactor compactor;

    private TimelineVersionedStore(
            Backend backend,
            TimelineVersionedStoreConfig config,
            CommitSeqBound bound,
            Db db,
            KeyTimelineIndex index,
            TimelineQuery query,
            RevisionRecordBuffer buffer,
            SessionWriter session,
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

    /** Delegates to {@link #restore(Backend, TimelineVersionedStoreConfig, BiConsumer)} with a no-op observer. */
    public static TimelineVersionedStore restore(Backend backend, TimelineVersionedStoreConfig config) {
        return restore(backend, config, (_, _) -> {});
    }

    /**
     * Reconstructs store state from the backend: replays all persisted revision records
     * into the timeline index, recovers the commit bounds, and resumes any in-progress
     * compaction batch before opening a fresh write session. Each revision record is
     * passed to {@code observer} in order, allowing callers to rebuild derived state
     * (e.g. lease key attachments) in a single pass.
     *
     * @param backend   the storage backend to restore from
     * @param config    store configuration
     * @param observer  called once per revision record in revision order; receives the
     *                  decoded {@link Revision} and its {@link Record}
     */
    public static TimelineVersionedStore restore(
            Backend backend,
            TimelineVersionedStoreConfig config,
            BiConsumer<Revision, Record> observer
    ) {
        var encoder = new RecordEncoder();
        var decoder = new RecordDecoder();
        var index = new KeyTimelineIndex(new PersistentBPlusTree<>(
                config.indexMaxKeys(),
                new PackedByteKeyStorageFactory(new LexigographicPackedByteComparator())
        ));

        var db = getDb(config);
        var txn = backend.beginRead();
        var tlTxn = index.txn();

        var bound = getBound(txn, db.meta());

        try (var it = txn.range(db.revision())) {
            for (var kv: it) {
                var revision = decoder.decodeRevision(kv.key());
                var record = decoder.decodeRecord(kv.val());

                tlTxn.restore(revision, record);
                observer.accept(revision, record);
            }
        }

        tlTxn.commit();

        tlTxn = index.txn();

        var buffer = RevisionRecordBuffer.allocate(config.maxRevisionRecordBuffer());
        var completedCompactionCommitSeq = commitSeq(txn, db.meta(), db.meta().completedCompactionCommitSeqKey());

        BatchCompactor compactor;

        if (bound.start() == completedCompactionCommitSeq) {
            compactor = BatchCompactor.completed();
        } else {
            var retained = tlTxn.compact(bound.start());
            compactor = BatchCompactor.create(txn, db, config.deleteBatchSize(), retained, decoder);
        }

        tlTxn.commit();
        txn.close();

        var query = new TimelineQuery();
        var session = new SessionWriter(config, db, backend.beginWrite(), index::txn, query, buffer, bound, encoder, decoder, compactor);

        return new TimelineVersionedStore(backend, config, bound, db, index, query, buffer, session, compactor, encoder, decoder);
    }

    private static Db getDb(TimelineVersionedStoreConfig config) {
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

    @Override
    public Reader reader() {
        // bound is advanced last in the write path (after buffer.publish and index.commit),
        // so reading it first guarantees buffer and index have reached at least lastCommitSeq.
        var lastCommitSeq = bound.end();
        var bufferView = buffer.view(lastCommitSeq);
        var tlView = index.view();
        var readTxn = backend.beginRead();
        return CommitBoundedReader.create(db, tlView, query, readTxn, bufferView, encoder, decoder, lastCommitSeq);
    }

    /** Swaps the shared buffer to a fresh instance; the volatile write is immediately visible to threads calling {@link #reader()}. */
    public void renewBuffer() {
        buffer = RevisionRecordBuffer.allocate(config.maxRevisionRecordBuffer());
    }

    @Override
    public CompactResult compact(long commitSeq) {
        if (!compactor.done()) {
            return new CompactResult.InProgress();
        }

        if (bound.start() >= commitSeq) {
            return new CompactResult.AlreadyCompacted(bound.start(), commitSeq);
        }

        if (bound.end() < commitSeq) {
            return new CompactResult.FutureRevision(bound.start(), commitSeq);
        }

        // Stage firstCommitSeq=N in the meta write txn and advance bound.start() in memory.
        // The change is not yet committed to the backend; new backend read txns still see the old boundary.
        session.compact(commitSeq);

        // --- reader created here [interleaving A] ---
        //
        //   backend   firstCommitSeq = old   persistedCommitSeq = old
        //   buffer    current session buffer (records up to bound.end())
        //   index     all revisions present
        //
        //   CommitBoundedReader reads firstCommitSeq from the backend txn; the write has
        //   not committed yet, so the old boundary is still in effect. This reader answers
        //   queries from old through bound.end() — it holds a snapshot taken before
        //   compaction was visible.

        // Flush the session: firstCommitSeq=N and all staged records land in the backend.
        // From this point any new reader's backend txn sees firstCommitSeq=N.
        session.commit();

        // --- reader created here [interleaving B] ---
        //
        //   backend   firstCommitSeq = N    persistedCommitSeq = E
        //             records [N..E] physically present
        //   buffer    pre-swap; may hold records below N from the just-committed session
        //   index     all revisions present (not yet compacted)
        //
        //   CommitBoundedReader reads firstCommitSeq=N from the backend txn.
        //   Buffer records below N are within the pinned view but are never returned —
        //   any query below N returns Compacted. Snapshot isolation holds.

        // Swap the buffer to a fresh instance; the volatile write is immediately visible to reader().
        renewBuffer();

        // --- reader created here [interleaving C] ---
        //
        //   backend   firstCommitSeq = N    persistedCommitSeq = E
        //             records [N..E] physically present
        //   buffer    fresh (empty)
        //   index     all revisions present (not yet compacted)
        //
        //   All data for [N..E] is exclusively in the backend. Index revisions below N
        //   still exist but are unreachable — firstCommitSeq=N gates all results.

        // Compact the timeline index to N via CoW: revisions strictly below N are pruned.
        // Concurrent readers hold their own index snapshots; the CoW update is atomic from their perspective.
        var tlTxn = index.txn();
        var retained = tlTxn.compact(commitSeq);
        tlTxn.commit();

        // --- reader created here [interleaving D] ---
        //
        //   backend   firstCommitSeq = N    persistedCommitSeq = E
        //             records [N..E] physically present (records below N pending deletion)
        //   buffer    fresh (empty)
        //   index     revisions [N..E] only
        //
        //   Full consistency: backend, buffer, and index all agree on N as the boundary.
        //   Backend records below N are physically present but unreachable;
        //   BatchCompactor will remove them incrementally over subsequent commits.

        var txn = backend.beginWrite();
        compactor = BatchCompactor.create(txn, db, config.deleteBatchSize(), retained, decoder);
        session = new SessionWriter(config, db, txn, index::txn, query, buffer, bound, encoder, decoder, compactor);

        return new CompactResult.Ok();
    }

    @Override
    public void sync() {
        // session.commit() flushes both the buffered revision records and one compaction batch step -
        // the compactor runs as part of every backend flush.
        session.commit();
        renewBuffer();
        session = new SessionWriter(config, db, backend.beginWrite(), index::txn, query, buffer, bound, encoder, decoder, compactor);
    }

    @Override
    public Writer writer() {
        if (session.commitIfExpired()) {
            // Session expired: flush it and start fresh.
            // A concurrent thread calling reader() may race between the buffer swap and the
            // new session start. Such a reader may hold the old buffer snapshot along with a
            // backend read txn that already reflects the just-flushed records. Because
            // CommitBoundedReader resolves records from the buffer first and falls back to the
            // backend, any overlap between the old buffer and the backend is harmless -
            // both carry identical data for the same revision.
            renewBuffer();
            session = new SessionWriter(config, db, backend.beginWrite(), index::txn, query, buffer, bound, encoder, decoder, compactor);
        }
        return session;
    }
}
