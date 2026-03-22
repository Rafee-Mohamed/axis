package io.disys.axis.storage.backend;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public interface Snapshot extends AutoCloseable {
    void writeTo(WritableByteChannel destination) throws IOException;
    long size();
}
