package io.disys.axis.mvcc.io;

import io.disys.axis.backend.WriteHandle;
import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.WriteTxn;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class WriteSession implements Writer {
    private final WriteTxn txn;
    private final KeyTimelineIndex index;
    private final RevisionRecordBuffer buffer;
    private final CommitSeqBound bound;
    private final VersionedStoreConfig config;
    private final RecordEncoder encoder;
    private final RecordDecoder decoder;
    private final VersionedStore.Db db;
    private final long expiryTime;
    private int ordinal;
    private final BatchCompactor compactor;

    public WriteSession(
            VersionedStoreConfig config,
            VersionedStore.Db db,
            WriteTxn txn,
            KeyTimelineIndex index,
            RevisionRecordBuffer buffer,
            CommitSeqBound bound,
            RecordEncoder encoder,
            RecordDecoder decoder,
            BatchCompactor compactor
    ) {
        this.config = config;
        this.txn = txn;
        this.index = index;
        this.buffer = buffer;
        this.bound = bound;
        this.encoder = encoder;
        this.decoder = decoder;
        this.db = db;
        this.ordinal = 0;
        this.expiryTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.revisionRecordBufferSyncTimeout());
        this.compactor = compactor;
    }

    boolean expired() {
        return buffer.size() >= config.maxRevisionRecordBuffer() ||
                System.nanoTime() >= expiryTime;
    }

    public boolean commitIfExpired() {
        if (expired()) {
            commit();
            return true;
        }

        return false;
    }

    public void commit() {
        close();
        txn.put(
                db.meta(),
                db.meta().persistedCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(bound.end()).array());
        compactor.compact(txn);
        txn.commit();
        txn.close();
    }

    @Override
    public void close() {
        if (ordinal == 0) {
            return;
        }
        ordinal = 0;
        buffer.publish();
        bound.advance();
    }

    private boolean compacted(long commitSeq) {
        return commitSeq < bound.start();
    }

    private boolean future(long commitSeq) {
        return commitSeq > bound.next();
    }

    public void compact(long commitSeq) {
        txn.put(
                db.meta(),
                db.meta().firstCommitSeqKey(),
                ByteBuffer.allocate(Long.BYTES).putLong(commitSeq).array());
        bound.compact(commitSeq);
    }

    @Override
    public void put(byte[] key, byte[] val) {
        var revision = Revision.modify(bound.next(), ordinal++);
        var span = index.add(key, revision);

        var record = new Record(key, val, span);
        buffer.stage(new RevisionRecord(revision, record));
        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
    }

    @Override
    public boolean delete(byte[] key) {
        var revision = Revision.modify(bound.next(), ordinal++);
        var span = index.complete(key, revision);

        if (span.isEmpty()) {
            return false;
        }

        var record = new Record(key, span.get());
        buffer.stage(new RevisionRecord(revision, record));
        txn.put(db.revision(), encoder.encode(revision), encoder.encode(record));
        return true;
    }

    @Override
    public int deleteRange(byte[] from, byte[] to) {
        var mapper = new BiFunction<byte[], KeyTimeline, Optional<RevisionRecord>>() {
            Revision revision = new Revision(bound.next(), ordinal);;
            @Override
            public Optional<RevisionRecord> apply(byte[] key, KeyTimeline timeline) {
                if (!timeline.tryComplete(revision)) {
                    return Optional.empty();
                }
                var nextRevisionRecord = new RevisionRecord(revision, new Record(key, timeline.lastSpan()));
                revision = revision.next();
                return Optional.of(nextRevisionRecord);
            }
        };

        index.range(from, to, mapper)
                .forEach(rr -> {
                    buffer.stage(rr);
                    txn.put(db.revision(), encoder.encode(rr.revision()), encoder.encode(rr.record()));
                });

        var deleted = mapper.revision.ordinal() - ordinal;
        ordinal = mapper.revision.ordinal();
        return deleted;
    }

    @Override
    public WriteHandle handle() {
        return txn;
    }

    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, bound.start(), bound.end()));
    }

    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(bound.next(), commitSeq);
        }

        return null;
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
            case KEY -> Comparator.comparing(Record::key, Arrays::compare);
            case VERSION -> Comparator.comparingInt(Record::version);
            case CREATED_REVISION -> Comparator.comparingLong(Record::createdAtSeq);
            case MODIFIED_REVISION -> Comparator.comparingLong(Record::modifiedAtSeq);
            case VAL -> Comparator.comparing(Record::val, Arrays::compare);
        };
        return options.sortDirection() == SortDirection.DESCENDING ? base.reversed() : base;
    }

    @Override
    public Optional<Record> get(byte[] key) {
        return index.revisionAt(key, bound.next()).map(r -> get(key, r));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.revisionAt(key, commitSeq).map(r -> get(key, r))
        );
    }

    // ===================== range =====================

    @Override
    public Page<Record> range(byte[] from, byte[] to) {
        var items = index.rangeAt(from, to, bound.next())
                .map(kr -> get(kr.key(), kr.revision()))
                .toList();
        return new Page<>(items, false);
    }

    @Override
    public Page<Record> range(byte[] from, byte[] to, RangeOptions options) {
        return pageRecords(index.rangeAt(from, to, bound.next()), options);
    }

    // ===================== rangeAt =====================

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new Page<>(index.rangeAt(from, to, commitSeq)
                        .map(kr -> get(kr.key(), kr.revision()))
                        .toList(), false)
        );
    }

    @Override
    public SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                pageRecords(index.rangeAt(from, to, commitSeq), options)
        );
    }

    // ===================== keys =====================

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to) {
        var items = index.rangeAt(from, to, bound.next())
                .map(KeyRevision::key)
                .toList();
        return new Page<>(items, false);
    }

    @Override
    public Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options) {
        return pageKeys(index.rangeAt(from, to, bound.next()), options);
    }

    // ===================== keysAt =====================

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                new Page<>(index.rangeAt(from, to, commitSeq)
                        .map(KeyRevision::key)
                        .toList(), false)
        );
    }

    @Override
    public SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options) {
        var result = this.<Page<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                pageKeys(index.rangeAt(from, to, commitSeq), options)
        );
    }

    // ===================== count =====================

    @Override
    public long count(byte[] from, byte[] to) {
        return index.countAt(from, to, bound.next()).count();
    }

    @Override
    public long count(byte[] from, byte[] to, CountOptions options) {
        if (!options.hasCreatedFilter() && !options.hasVersionFilter()) {
            var stream = index.countAt(from, to, bound.next());
            if (options.hasModifiedFilter()) {
                stream = stream.filter(r -> options.modifiedIn().test(r.commitSeq()));
            }
            return stream.count();
        }
        Stream<KeyRevision> krStream = index.rangeAt(from, to, bound.next());
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
                index.countAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        if (result != null) return result;

        if (!options.hasCreatedFilter() && !options.hasVersionFilter()) {
            var stream = index.countAt(from, to, commitSeq);
            if (options.hasModifiedFilter()) {
                stream = stream.filter(r -> options.modifiedIn().test(r.commitSeq()));
            }
            return new SnapshotResult.Ok<>(stream.count());
        }
        Stream<KeyRevision> krStream = index.rangeAt(from, to, commitSeq);
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
}
