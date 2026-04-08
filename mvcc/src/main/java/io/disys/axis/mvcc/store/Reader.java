package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

import java.util.Optional;

public interface Reader extends AutoCloseable {
    Optional<Record> get(byte[] key);
    SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq);

    RecordIterator range(byte[] from, byte[] to);
    RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound);
    RecordIterator range(byte[] from, byte[] to, long limit);
    RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit);

    SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq,  ModifiedAtSeqBound bound);
    SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, long limit);
    SnapshotResult<RecordIterator> rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit);

    KeyIterator keys(byte[] from, byte[] to);
    KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound);
    KeyIterator keys(byte[] from, byte[] to, long limit);
    KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit);

    SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound);
    SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, long limit);
    SnapshotResult<KeyIterator> keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit);

    @Override
    void close();
}
