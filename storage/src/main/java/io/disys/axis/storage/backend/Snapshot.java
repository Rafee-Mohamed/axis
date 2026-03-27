package io.disys.axis.storage.backend;

import java.io.IOException;
import java.nio.file.Path;

public interface Snapshot extends AutoCloseable {
    void take() throws IOException;
    long size();
}
