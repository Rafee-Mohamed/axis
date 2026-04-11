package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.WriteTxn;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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
                ByteBuffer.allocate(Long.BYTES)
                        .putLong(commitSeq)
                        .array());
        bound.compact(commitSeq);
    }
    
    @Override
    public void put(byte[] key, byte[] val)  {
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


    private <T> SnapshotResult<T> snapshotResultOutsideWindow(long commitSeq) {
        if (compacted(commitSeq)) {
            return new SnapshotResult.Compacted<>(bound.start(), commitSeq);
        }

        if (future(commitSeq)) {
            return new SnapshotResult.Future<>(bound.next(), commitSeq);
        }

        return null;
    }


    Record get(byte[] key, Revision revision) {
        return buffer.get(revision)
                .map(RevisionRecord::record)
                .or(() -> txn.get(db.revision(), encoder.encode(revision))
                        .map(decoder::decodeRecord))
                .orElseThrow(() ->
                        new InconsistentStoreException.MissingRecordForRevision(key, revision, bound.start(), bound.end()));
    }

    @Override
    public Optional<Record> get(byte[] key) {
        return index.revisionAt(key, bound.next())
                .map(r -> get(key, r));
    }

    @Override
    public SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq) {
        var result = this.<Optional<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.revisionAt(key, commitSeq)
                        .map(r -> get(key, r))
        );
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to) {
        return index.rangeAt(from, to, bound.next())
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.rangeAt(from, to, bound.next())
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, long limit) {
        return index.rangeAt(from, to, bound.next())
                .limit(limit)
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return index.rangeAt(from, to, bound.next())
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .limit(limit)
                .map(kr -> get(kr.key(), kr.revision()))
                ::iterator;
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .limit(limit)
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<Iterable<Record>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result :  new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(kr -> get(kr.key(), kr.revision()))
                        ::iterator        
        );
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to) {
        return index.rangeAt(from, to, bound.next())
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.rangeAt(from, to, bound.next())
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, long limit) {
        return index.rangeAt(from, to, bound.next())
                .limit(limit)
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        return index.rangeAt(from, to, bound.next())
                .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                .limit(limit)
                .map(KeyRevision::key)
                ::iterator;
    }

    @Override
    public SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .limit(limit)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public SnapshotResult<Iterable<byte[]>>  keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound, long limit) {
        var result = this.<Iterable<byte[]>>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.rangeAt(from, to, commitSeq)
                        .filter(kr -> modifiedAtSeqBound.test(kr.revision()))
                        .limit(limit)
                        .map(KeyRevision::key)
                        ::iterator
        );
    }

    @Override
    public long count(byte[] from, byte[] to) {
        return index.countAt(from, to, bound.next()).count();
    }

    @Override
    public long count(byte[] from, byte[] to, ModifiedAtSeqBound modifiedAtSeqBound) {
        return index.countAt(from, to, bound.next()).filter(modifiedAtSeqBound).count();
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.countAt(from, to, commitSeq).count()
        );
    }

    @Override
    public SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound modifiedAtSeqBound) {
        var result = this.<Long>snapshotResultOutsideWindow(commitSeq);
        return result != null ? result : new SnapshotResult.Ok<>(
                index.countAt(from, to, commitSeq).filter(modifiedAtSeqBound).count()
        );
    }

}
