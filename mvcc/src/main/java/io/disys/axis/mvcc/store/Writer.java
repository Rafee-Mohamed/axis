package io.disys.axis.mvcc.store;

import io.disys.axis.backend.WriteHandle;

public interface Writer extends Reader, AutoCloseable {
    void put(byte[] key, byte[] val);
    boolean delete(byte[] key);
    int deleteRange(byte[] from, byte[] to);

    WriteHandle handle();
    @Override
    void close();
}
