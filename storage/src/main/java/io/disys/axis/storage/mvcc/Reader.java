package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Reader extends AutoCloseable {
    ReadResult get(byte[] key) throws IOException, InconsistentStoreException;
    ReadResult getAt(byte[] key, long commitSeq) throws IOException, InconsistentStoreException;
    CloseableIterator<Record> range(byte[] startKey, byte[] endKey);
    CloseableIterator<Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq);
    @Override
    void close() throws IOException;
}
