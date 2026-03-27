package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Reader extends AutoCloseable {
    ReadResult get(byte[] key) throws IOException;
    CloseableIterator<Record> range(byte[] startKey, byte[] endKey);
    @Override
    void close() throws IOException;
}
