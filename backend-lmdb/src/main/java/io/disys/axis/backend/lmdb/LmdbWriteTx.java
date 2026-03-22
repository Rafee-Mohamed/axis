package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.Database;
import io.disys.axis.storage.backend.KeyValue;
import io.disys.axis.storage.backend.WriteTx;

final class LmdbWriteTx implements WriteTx {
    @Override
    public void put(Database db, byte[] key, byte[] value) {
    }

    @Override
    public void delete(Database db, byte[] key) {
    }

    @Override
    public byte[] get(Database db, byte[] key) {
        return null;
    }

    @Override
    public CloseableIterator<KeyValue> range(Database db, byte[] start, byte[] end) {
        return null;
    }

    @Override
    public void commit() {
    }

    @Override
    public void close() {
    }
}
