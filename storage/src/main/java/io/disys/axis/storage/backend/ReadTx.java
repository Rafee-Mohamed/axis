package io.disys.axis.storage.backend;

public interface ReadTx extends AutoCloseable {
    byte[] get(Database db, byte[] key);
    CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end);
}
