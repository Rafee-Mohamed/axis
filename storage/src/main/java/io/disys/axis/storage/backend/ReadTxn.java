package io.disys.axis.storage.backend;

import java.util.Optional;

public interface ReadTxn extends AutoCloseable {
    Optional<byte[]> get(Database db, byte[] key);
    CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end);
}
