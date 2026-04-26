package io.disys.axis.mvcc.store;

import io.disys.axis.backend.WriteHandle;
import io.disys.axis.mvcc.model.Record;

import java.util.List;
import java.util.Optional;

public interface Writer extends Reader, AutoCloseable {
    void put(byte[] key, byte[] val);
    Optional<Record> putAndGet(byte[] key, byte[] val);

    boolean delete(byte[] key);
    Optional<Record> deleteAndGet(byte[] key);

    int deleteRange(byte[] from, byte[] to);
    List<Record> deleteRangeAndGet(byte[] from, byte[] to);

    WriteHandle handle();
    @Override
    void close();
}
