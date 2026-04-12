package io.disys.axis.backend;

import java.util.Optional;

public interface ReadHandle {
    Optional<byte[]> get(Database db, byte[] key);
    CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end);
    CloseableIterator<KeyVal> range(Database db);
}
