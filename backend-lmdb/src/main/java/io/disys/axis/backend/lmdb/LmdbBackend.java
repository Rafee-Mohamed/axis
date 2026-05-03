package io.disys.axis.backend.lmdb;

import io.disys.axis.backend.*;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class LmdbBackend implements Backend {
    private final Env<ByteBuffer> env;
    private final Map<String, Dbi<ByteBuffer>> dbs;
    private final LmdbConfig config;

    public LmdbBackend(LmdbConfig config) {
        var dirFile = config.directory().toFile();
        dirFile.mkdirs();

        this.env = Env.create()
                .setMapSize(config.mapSize())
                .setMaxDbs(config.databases().size())
                .open(dirFile);

        this.dbs = new HashMap<>();

        for (var db : config.databases()) {
            dbs.put(db.name(), env.openDbi(db.name(), DbiFlags.MDB_CREATE));
        }

        this.config = config;
    }

    @Override
    public WriteTxn beginWrite() {
        var txn = env.txnWrite();
        return new LmdbWriteTxn(txn, dbs);
    }

    @Override
    public ReadTxn beginRead() {
        var txn = env.txnRead();
        return new LmdbReadTxn(txn, dbs);
    }

    @Override
    public Snapshot snapshot() {
        return new LmdbSnapshot(env, config.snapshotDestination());
    }

    @Override
    public void close() {
        env.close();
    }
}
