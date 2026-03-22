package io.disys.axis.wal;

import java.nio.ByteBuffer;

public interface IndexExtractor {
    long extract(ByteBuffer payload);
}
