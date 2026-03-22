package io.disys.axis.backend.lmdb;

import io.disys.axis.storage.backend.Snapshot;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

final class LmdbSnapshot implements Snapshot {
    @Override
    public void writeTo(WritableByteChannel destination) throws IOException {
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() {
    }
}
