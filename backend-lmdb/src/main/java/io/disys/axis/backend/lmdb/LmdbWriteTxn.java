package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.Database;
import io.disys.axis.storage.backend.KeyVal;
import io.disys.axis.storage.backend.WriteTxn;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

final class LmdbWriteTxn implements WriteTxn {
    private final Txn<ByteBuffer> txn;
    private final Map<Database, Dbi<ByteBuffer>> dbs;

    LmdbWriteTxn(Txn<ByteBuffer> txn, Map<Database, Dbi<ByteBuffer>> dbs) {
        this.txn = txn;
        this.dbs = dbs;
    }

    @Override
    public void put(Database db, byte[] key, byte[] value) {
        dbs.get(db).put(txn, ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    @Override
    public void delete(Database db, byte[] key) {
        dbs.get(db).delete(txn, ByteBuffer.wrap(key));
    }

    @Override
    public Optional<byte[]> get(Database db, byte[] key) {
        var val = dbs.get(db).get(txn, ByteBuffer.wrap(key));
        if (val == null) return Optional.empty();
        return Optional.of(LmdbUtil.toBytes(val));
    }

    @Override
    public CloseableIterator<KeyVal> range(Database db, byte[] start, byte[] end) {
        return new LmdbClosableIterator(
                dbs.get(db).iterate(
                        txn,
                        KeyRange.closed(ByteBuffer.wrap(start), ByteBuffer.wrap(end))
                )
        );
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
