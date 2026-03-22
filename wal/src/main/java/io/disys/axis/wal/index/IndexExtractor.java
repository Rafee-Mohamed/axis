package io.disys.axis.wal.index;

import java.nio.ByteBuffer;

public interface IndexExtractor {
    long extract(ByteBuffer payload);
}
