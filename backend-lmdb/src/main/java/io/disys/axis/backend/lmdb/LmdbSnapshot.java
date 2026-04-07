package io.disys.axis.backend.lmdb;

import io.disys.axis.backend.Snapshot;
import org.lmdbjava.Env;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

final class LmdbSnapshot implements Snapshot {

    private final Env<ByteBuffer> env;
    private final Path destination;

    LmdbSnapshot(Env<ByteBuffer> env, Path destination) {
        this.env = env;
        this.destination = destination;
    }

    @Override
    public void take() {
        env.copy(destination.toFile());
    }

    @Override
    public long size() {
        return destination.toFile().length();
    }

    @Override
    public void close() {
    }
}
