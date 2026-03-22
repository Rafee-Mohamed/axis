package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.Database;
import io.disys.axis.storage.backend.KeyValue;
import io.disys.axis.storage.backend.ReadTx;

final class LmdbReadTx implements ReadTx {
    @Override
    public byte[] get(Database db, byte[] key) {
        return null;
    }

    @Override
    public CloseableIterator<KeyValue> range(Database db, byte[] start, byte[] end) {
        return null;
    }

    @Override
    public void close() {
    }
}
