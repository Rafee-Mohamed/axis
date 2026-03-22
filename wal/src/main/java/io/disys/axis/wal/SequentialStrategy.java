package io.disys.axis.wal;

import java.nio.ByteBuffer;

public class SequentialStrategy implements IndexStrategy {

    @Override
    public long index(long seq, ByteBuffer payload) {
        return seq;
    }
}
