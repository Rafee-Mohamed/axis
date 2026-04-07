package io.disys.axis.backend;

import java.io.IOException;
import java.util.Optional;

public interface WriteTxn extends ReadTxn, AutoCloseable {
    void put(Database db, byte[] key, byte[] value);
    void delete(Database db, byte[] key);
    void commit();
    @Override
    void close();
}
