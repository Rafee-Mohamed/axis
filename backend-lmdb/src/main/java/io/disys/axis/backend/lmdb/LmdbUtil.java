package io.disys.axis.backend.lmdb;

import java.nio.ByteBuffer;

public class LmdbUtil {
    private LmdbUtil() {}

    static byte[] toBytes(ByteBuffer buf) {
        byte[] copy = new byte[buf.remaining()];
        buf.get(copy);
        return copy;
    }
}
