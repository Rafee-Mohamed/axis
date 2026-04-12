package io.disys.axis.backend;

public interface WriteHandle extends ReadHandle {
    void put(Database db, byte[] key, byte[] value);
    void delete(Database db, byte[] key);
}
