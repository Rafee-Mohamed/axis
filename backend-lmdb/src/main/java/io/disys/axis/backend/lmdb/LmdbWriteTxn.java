package io.disys.axis.backend.lmdb;

import io.disys.axis.backend.CloseableIterator;
import io.disys.axis.backend.Database;
import io.disys.axis.backend.KeyVal;
import io.disys.axis.backend.WriteTxn;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

final class LmdbWriteTxn implements WriteTxn {
    private final Txn<ByteBuffer> txn;
    private final Map<String, Dbi<ByteBuffer>> dbs;

    LmdbWriteTxn(Txn<ByteBuffer> txn, Map<String, Dbi<ByteBuffer>> dbs) {
        this.txn = txn;
        this.dbs = dbs;
    }

    @Override
    public void put(Database db, byte[] key, byte[] value) {
        dbs.get(db.name()).put(txn, LmdbUtil.direct(key), LmdbUtil.direct(value));
    }

    @Override
    public void delete(Database db, byte[] key) {
        dbs.get(db.name()).delete(txn, LmdbUtil.direct(key));
    }

    @Override
    public Optional<byte[]> get(Database db, byte[] key) {
        var val = dbs.get(db.name()).get(txn, LmdbUtil.direct(key));
        if (val == null) return Optional.empty();
        return Optional.of(LmdbUtil.toBytes(val));
    }

    @Override
    public CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end) {
        return new LmdbClosableIterator(
                dbs.get(db.name()).iterate(
                        txn,
                        KeyRange.closed(LmdbUtil.direct(start), LmdbUtil.direct(end))
                )
        );
    }

    @Override
    public CloseableIterator<KeyVal> range(Database db) {
        return new LmdbClosableIterator(dbs.get(db.name()).iterate(txn));
    }

    @Override
    public void commit() {
        txn.commit();
    }

    @Override
    public void close() {
        txn.close();
    }
}
