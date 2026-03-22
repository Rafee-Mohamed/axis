package io.disys.axis.wal;

import java.nio.ByteBuffer;

public interface IndexStrategy {
    long index(long seq, ByteBuffer payload);
}
