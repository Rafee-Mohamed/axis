package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

import java.util.Optional;

public interface Reader extends AutoCloseable {
    Optional<Record> get(byte[] key);
    ReadResult getAt(byte[] key, long commitSeq);
    RecordIterator range(byte[] startKey, byte[] endKey);
    RangeResult rangeAt(byte[] startKey, byte[] endKey, long commitSeq);
    @Override
    void close();
}
