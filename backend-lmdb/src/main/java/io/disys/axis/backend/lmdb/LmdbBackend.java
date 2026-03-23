package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.*;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LmdbBackend implements Backend {
    private final Env<ByteBuffer> env;
    private final Map<Database, Dbi<ByteBuffer>> dbs;

    public LmdbBackend(LmdbConfig config) {
        var dirFile = config.directory().toFile();
        dirFile.mkdirs();

        this.env = Env.create()
                .setMapSize(config.mapSize())
                .setMaxDbs(config.databases().size())
                .open(dirFile);

        this.dbs = new HashMap<>();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (var db: config.databases()) {
                var dbi = env.openDbi(db.name(), DbiFlags.MDB_CREATE);
                dbs.put(db, dbi);
            }
            txn.commit();
        }
    }

    @Override
    public WriteTx beginWrite() {
        var txn = env.txnWrite();
        return new LmdbWriteTx(txn, dbs);
    }

    @Override
    public ReadTx beginRead() {
        var txn = env.txnRead();
        return new LmdbReadTx(txn, dbs);
    }

    @Override
    public Snapshot snapshot() {
        return new LmdbSnapshot();
    }

    @Override
    public void close() {
        env.close();
    }
}
