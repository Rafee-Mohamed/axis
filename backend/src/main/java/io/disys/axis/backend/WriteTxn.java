package io.disys.axis.backend;

import java.io.IOException;
import java.util.Optional;

public interface WriteTxn extends AutoCloseable {
    void put(Database db, byte[] key, byte[] value);
    void delete(Database db, byte[] key);
    Optional<byte[]> get(Database db, byte[] key);
    CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end);
    void commit() throws IOException;
    @Override
    void close() throws IOException;
}
