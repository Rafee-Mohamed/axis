package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Writer extends AutoCloseable {
    void put(byte[] key, byte[] val) throws IOException;
    boolean delete(byte[] key) throws IOException;
    Optional<Record> get(byte[] key) throws IOException;
    CloseableIterator<KeyVal> range(byte[] key, byte[] val);
    @Override
    void close() throws IOException;
}
