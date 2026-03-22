package io.disys.axis.storage.backend;

public interface WriteTx extends AutoCloseable {
    void put(Database db, byte[] key, byte[] value);
    void delete(Database db, byte[] key);
    byte[] get(Database db, byte[] key);
    CloseableIterator<KeyValue> range(Database db, byte[] start, byte[] end);
    void commit();
}
