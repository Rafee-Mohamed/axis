package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.CloseableIterator;
import io.disys.axis.storage.backend.Database;
import io.disys.axis.storage.backend.KeyVal;
import io.disys.axis.storage.backend.ReadTx;
import org.lmdbjava.Dbi;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.Map;

final class LmdbReadTx implements ReadTx {
    private final Txn<ByteBuffer> txn;
    private final Map<Database, Dbi<ByteBuffer>> dbs;

    LmdbReadTx(Txn<ByteBuffer> txn, Map<Database, Dbi<ByteBuffer>> dbs) {
        this.txn = txn;
        this.dbs = dbs;
    }

    @Override
    public byte[] get(Database db, byte[] key) {
        var val = dbs.get(db).get(txn, ByteBuffer.wrap(key));
        if (val == null) return null;
        return LmdbUtil.toBytes(val);
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
    public void close() {
        txn.close();
    }
}
