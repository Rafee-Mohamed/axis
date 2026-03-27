package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Writer extends Reader, AutoCloseable {
    void put(byte[] key, byte[] val) throws IOException;
    boolean delete(byte[] key) throws IOException;
    @Override
    void close() throws IOException;
}
