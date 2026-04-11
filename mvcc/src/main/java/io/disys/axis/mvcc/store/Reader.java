package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

import java.util.Optional;

public interface Reader extends AutoCloseable {
    Optional<Record> get(byte[] key);
    SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq);

    Iterable<Record> range(byte[] from, byte[] to);
    Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound bound);
    Iterable<Record> range(byte[] from, byte[] to, long limit);
    Iterable<Record> range(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit);

    SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq,  ModifiedAtSeqBound bound);
    SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, long limit);
    SnapshotResult<Iterable<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit);

    Iterable<byte[]> keys(byte[] from, byte[] to);
    Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound bound);
    Iterable<byte[]> keys(byte[] from, byte[] to, long limit);
    Iterable<byte[]> keys(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit);

    SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound);
    SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, long limit);
    SnapshotResult<Iterable<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit);

    long count(byte[] from, byte[] to);
    long count(byte[] from, byte[] to, ModifiedAtSeqBound bound);

    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound);

    @Override
    void close();
}
