package io.disys.axis.mvcc.io;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.ReadTxn;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

public class CommitBoundedReader implements Reader {

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
    private final long lastCommitSeq;

    private CommitBoundedReader(
            VersionedStore.Db db,
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer.View buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            long firstCommitSeq,
            long lastCommitSeq
    ) {
        this.db = db;
        this.index = index;
        this.txn = txn;
        this.buffer = buffer;
        this.encoder = encoder;
        this.decoder = decoder;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
    }

    private static long getCommitSeq(ReadTxn txn, VersionedStore.MetaDb db, byte[] key) {
        return txn.get(db, key)
                .map(b -> ByteBuffer.allocate(Long.BYTES).put(b).flip().getLong())
                .orElse(0L);
    }

    public static CommitBoundedReader create(
            VersionedStore.Db db,
            KeyTimelineIndex index,
            ReadTxn txn,
            RevisionRecordBuffer buffer,
            RecordEncoder encoder,
            RecordDecoder decoder,
            CommitSeqBound bound
    ) {
        var firstCommitSeq = getCommitSeq(txn, db.meta(), db.meta().firstCommitSeqKey());
        var lastCommitSeq = bound.end();
        return new CommitBoundedReader(
                db,
                index,
                txn,
                buffer.view(lastCommitSeq),
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

    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, firstCommitSeq, lastCommitSeq));
    }

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
        return index.pinnedRevisionAt(key, lastCommitSeq)
                .map(r -> get(key, r));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedRevisionAt(key, commitSeq)
                        .map(r -> get(key, r))
        );
    }

    // ===================== Private helpers =====================

    private Page<Record> pageRecords(Stream<KeyRevision> stream, RangeOptions options) {
        if (options.hasModifiedFilter()) {
            stream = stream.filter(kr -> options.modifiedIn().test(kr.revision().commitSeq()));
        }
        Stream<Record> records = stream.map(kr -> get(kr.key(), kr.revision()));
        if (options.hasCreatedFilter()) {
            records = records.filter(r -> options.createdIn().test(r.createdAtSeq()));
        }
        if (options.hasVersionFilter()) {
            records = records.filter(r -> options.versionIn().test(r.version()));
        }
        if (!options.isKeySort()) {
            records = records.sorted(recordComparator(options));
        }
        return page(records, options.limit());
    }

    private Page<byte[]> pageKeys(Stream<KeyRevision> stream, RangeOptions options) {
        if (options.hasModifiedFilter()) {
            stream = stream.filter(kr -> options.modifiedIn().test(kr.revision().commitSeq()));
        }
        // KEY sort without createdIn and without versionIn: never need to load records
        if (options.isKeySort() && !options.hasCreatedFilter() && !options.hasVersionFilter()) {
            return page(stream.map(KeyRevision::key), options.limit());
        }
        Stream<Record> records = stream.map(kr -> get(kr.key(), kr.revision()));
        if (options.hasCreatedFilter()) {
            records = records.filter(r -> options.createdIn().test(r.createdAtSeq()));
        }
        if (options.hasVersionFilter()) {
            records = records.filter(r -> options.versionIn().test(r.version()));
        }
        if (!options.isKeySort()) {
            records = records.sorted(recordComparator(options));
        }
        return page(records.map(Record::key), options.limit());
    }

    private <T> Page<T> page(Stream<T> stream, long limit) {
        if (limit == RangeOptions.UNLIMITED) {
            return new Page<>(stream.toList(), false);
        }
        var items = stream.limit(limit + 1).toList();
        boolean more = items.size() > limit;
        return new Page<>(more ? items.subList(0, (int) limit) : items, more);
    }

    private static Comparator<Record> recordComparator(RangeOptions options) {
        Comparator<Record> base = switch (options.sortTarget()) {
            case KEY               -> Comparator.comparing(Record::key, Arrays::compare);
            case VERSION           -> Comparator.comparingInt(Record::version);
            case CREATED_REVISION  -> Comparator.comparingLong(Record::createdAtSeq);
            case MODIFIED_REVISION -> Comparator.comparingLong(Record::modifiedAtSeq);
            case VAL               -> Comparator.comparing(Record::val, Arrays::compare);
        };
        return options.sortDirection() == SortDirection.DESCENDING ? base.reversed() : base;
    }

    // ===================== range =====================

    @Override
    public Page<Record> range(byte[] from, byte[] to) {
        var items = index.pinnedRangeAt(from, to, lastCommitSeq)
                .map(kr -> get(kr.key(), kr.revision()))
                .toList();
        return new Page<>(items, false);
    }

    @Override
    public Page<Record> range(byte[] from, byte[] to, RangeOptions options) {
        return pageRecords(index.pinnedRangeAt(from, to, lastCommitSeq), options);
    }

    // ===================== rangeAt =====================

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new Page<>(index.pinnedRangeAt(from, to, commitSeq)
                        .map(kr -> get(kr.key(), kr.revision()))
                        .toList(), false)
        );
    }

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                pageRecords(index.pinnedRangeAt(from, to, commitSeq), options)
        );
    }

    // ===================== keys =====================

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to) {
        var items = index.pinnedRangeAt(from, to, lastCommitSeq)
                .map(KeyRevision::key)
                .toList();
        return new Page<>(items, false);
    }

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options) {
        return pageKeys(index.pinnedRangeAt(from, to, lastCommitSeq), options);
    }

    // ===================== keysAt =====================

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new Page<>(index.pinnedRangeAt(from, to, commitSeq)
                        .map(KeyRevision::key)
                        .toList(), false)
        );
    }

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                pageKeys(index.pinnedRangeAt(from, to, commitSeq), options)
        );
    }

    // ===================== count =====================

    @Override
    public long count(byte[] from, byte[] to) {
        return index.pinnedCountAt(from, to, lastCommitSeq).count();
    }

    @Override
    public long count(byte[] from, byte[] to, CountOptions options) {
        if (!options.hasCreatedFilter() && !options.hasVersionFilter()) {
            var stream = index.pinnedCountAt(from, to, lastCommitSeq);
            if (options.hasModifiedFilter()) {
                stream = stream.filter(r -> options.modifiedIn().test(r.commitSeq()));
            }
            return stream.count();
        }
        Stream<KeyRevision> krStream = index.pinnedRangeAt(from, to, lastCommitSeq);
        if (options.hasModifiedFilter()) {
            krStream = krStream.filter(kr -> options.modifiedIn().test(kr.revision().commitSeq()));
        }
        Stream<Record> records = krStream.map(kr -> get(kr.key(), kr.revision()));
        if (options.hasCreatedFilter()) {
            records = records.filter(r -> options.createdIn().test(r.createdAtSeq()));
        }
        if (options.hasVersionFilter()) {
            records = records.filter(r -> options.versionIn().test(r.version()));
        }
        return records.count();
    }

    // ===================== countAt =====================

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.pinnedCountAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        if (result != null) return result;

        if (!options.hasCreatedFilter() && !options.hasVersionFilter()) {
            var stream = index.pinnedCountAt(from, to, commitSeq);
            if (options.hasModifiedFilter()) {
                stream = stream.filter(r -> options.modifiedIn().test(r.commitSeq()));
            }
            return new SnapshotResult.Ok<>(stream.count());
        }
        Stream<KeyRevision> krStream = index.pinnedRangeAt(from, to, commitSeq);
        if (options.hasModifiedFilter()) {
            krStream = krStream.filter(kr -> options.modifiedIn().test(kr.revision().commitSeq()));
        }
        Stream<Record> records = krStream.map(kr -> get(kr.key(), kr.revision()));
        if (options.hasCreatedFilter()) {
            records = records.filter(r -> options.createdIn().test(r.createdAtSeq()));
        }
        if (options.hasVersionFilter()) {
            records = records.filter(r -> options.versionIn().test(r.version()));
        }
        return new SnapshotResult.Ok<>(records.count());
    }

    // ===================== handle / close =====================

    @Override
    public ReadHandle handle() { return txn; }

    @Override
    public void close() { txn.close(); }
}
