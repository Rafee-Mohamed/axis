package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.Backend;
import io.disys.axis.storage.backend.ReadTx;
import io.disys.axis.storage.backend.Snapshot;
import io.disys.axis.storage.backend.WriteTx;

public class LmdbBackend implements Backend {
    @Override
    public WriteTx beginWrite() {
        return new LmdbWriteTx();
    }

    @Override
    public ReadTx beginRead() {
        return new LmdbReadTx();
    }

    @Override
    public Snapshot snapshot() {
        return new LmdbSnapshot();
    }

    @Override
    public void close() {
    }
}
