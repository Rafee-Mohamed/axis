package io.disys.axis.mvcc.io;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.ReadTxn;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * {@link Reader} implementation bounded to a fixed commit window
 * {@code [firstCommitSeq, lastCommitSeq]}.
 *
 * <p>Records are resolved buffer-first: the pinned {@link RevisionRecordBuffer.View} is
 * checked before the backend {@link ReadTxn}, bridging the gap between logical commits
 * and backend flushes. The backend covers {@code [firstCommitSeq, persistedCommitSeq]};
 * the buffer covers at minimum {@code (persistedCommitSeq, lastCommitSeq]}.</p>
 *
 * <p>Reads below {@code firstCommitSeq} return {@link SnapshotResult.Compacted};
 * reads above {@code lastCommitSeq} return {@link SnapshotResult.Future}.</p>
 */
public class CommitBoundedReader implements Reader {

    /**
     * Snapshot of the key timeline index taken at reader creation; contains at least all
     * revisions up to {@code lastCommitSeq}. Resolves which revision of a key was current
     * at a given commitSeq.
     */
    private final TimelineView view;

    private final TimelineQuery query;

    /** Backend read transaction; authoritative for {@code [firstCommitSeq, persistedCommitSeq]}. */
    private final ReadTxn txn;

    /**
     * Pinned buffer snapshot bounded to {@code lastCommitSeq}. Covers at minimum
     * {@code (persistedCommitSeq, lastCommitSeq]}; may also hold records at or before
     * {@code persistedCommitSeq} not yet flushed to the backend, and records beyond
     * {@code lastCommitSeq} staged by a concurrent writer — both are outside the read
     * window and ignored.
     */
    private final RevisionRecordBuffer.View buffer;

    private final RecordEncoder encoder;

    private final RecordDecoder decoder;

    private final Db db;

    /** Inclusive lower bound; the compaction boundary. Requests below this are Compacted. */
    private final long firstCommitSeq;

    /** Inclusive upper bound; this reader's fixed commitSeq. */
    private final long lastCommitSeq;

    private CommitBoundedReader(
            Db db,
            TimelineView view,
            TimelineQuery query,
            ReadTxn txn,
            RevisionRecordBuffer.View buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long firstCommitSeq,
            long lastCommitSeq
    ) {
        this.db = db;
        this.view = view;
        this.query = query;
        this.txn = txn;
        this.buffer = buffer;
        this.encoder = encoder;
        this.decoder = decoder;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
    }

    private static long getCommitSeq(ReadTxn txn, MetaDb db, byte[] key) {
        return txn.get(db, key)
                .map(b -> ByteBuffer.allocate(Long.BYTES).put(b).flip().getLong())
                .orElse(0L);
    }

    /**
     * Assembles a reader from pre-pinned components, reading {@code firstCommitSeq} from backend meta.
     */
    public static CommitBoundedReader create(
            Db db,
            TimelineView tlView,
            TimelineQuery query,
            ReadTxn txn,
            RevisionRecordBuffer.View bufferView,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long lastCommitSeq
    ) {
        var firstCommitSeq = getCommitSeq(txn, db.meta(), db.meta().firstCommitSeqKey());
        return new CommitBoundedReader(
                db,
                tlView,
                query,
                txn,
                bufferView,
                encoder,
                decoder,
                firstCommitSeq,
                lastCommitSeq
        );
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < firstCommitSeq;
    }

    private boolean future(long commitSeq) {
        return commitSeq > lastCommitSeq;
    }

    /**
     * Resolves a record by checking the buffer first, then the backend transaction.
     * The buffer covers recently committed records not yet flushed to the backend.
     */
    Record get(RevisionData data) {
        return buffer.get(data.revision())
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(data.revision()))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new IllegalStateException("CommitBoundedReader: Record missing for timeline-selected revision: revision=%s, revision bounds=[%d..%d]"
                                .formatted(data.revision(), firstCommitSeq, lastCommitSeq)));
    }

    Record get(KeyRevisionData krd) {
        return get(krd.data());
    }

    /**
     * @return Compacted if {@code commitSeq} is below the compaction boundary,
     *         Future if above the reader's fixed seq, null if within the visible window
     */
    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(firstCommitSeq, commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(lastCommitSeq, commitSeq);
        }

        return null;
    }

    // ===================== get / getAt =====================

    @Override
    public Optional<Record> get(byte[] key) {
        return view.getAt(key, lastCommitSeq).map(this::get);
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                view.getAt(key, commitSeq).map(this::get)
        );
    }

    // ===================== range =====================

    @Override
    public Page<Record> range(byte[] from, byte[] to) {
        return query.page(view.rangeAt(from, to, lastCommitSeq), this::get);
    }

    @Override
    public Page<Record> range(byte[] from, byte[] to, RangeOptions options) {
        return query.page(
                view.rangeAt(from, to, lastCommitSeq, options.sortDirection()),
                this::get,
                options
        );
    }

    // ===================== rangeAt =====================

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(view.rangeAt(from, to, commitSeq), this::get)
        );
    }

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.page(
                        view.rangeAt(from, to, commitSeq, options.sortDirection()),
                        this::get,
                        options
                )
        );
    }

    // ===================== keys =====================

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to) {
        return query.pageKeys(view.rangeAt(from, to, lastCommitSeq));
    }

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options) {
        return query.pageKeys(view.rangeAt(from, to, lastCommitSeq), this::get, options);
    }

    // ===================== keysAt =====================

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(view.rangeAt(from, to, commitSeq))
        );
    }

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                query.pageKeys(view.rangeAt(from, to, commitSeq), this::get, options)
        );
    }

    // ===================== count =====================

    @Override
    public long count(byte[] from, byte[] to) {
        return view.rangeAt(from, to, lastCommitSeq).count();
    }

    @Override
    public long count(byte[] from, byte[] to, CountOptions options) {
        return query.count(view.rangeAt(from, to, lastCommitSeq), options);
    }

    // ===================== countAt =====================

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                view.rangeAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        if (result != null) return result;
        return new SnapshotResult.Ok<>(query.count(view.rangeAt(from, to, commitSeq), options));
    }

    @Override
    public long revision() {
        return lastCommitSeq;
    }

    // ===================== handle / close =====================

    @Override
    public ReadHandle handle() { return txn; }

    @Override
    public void close() { txn.close(); }
}
