package io.disys.axis.backend;

import java.io.IOException;
import java.util.Optional;

public interface ReadTxn extends AutoCloseable {
    Optional<byte[]> get(Database db, byte[] key);
    CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end);
    CloseableIterator<KeyVal> range(Database db);
    @Override
    void close();
}
