package io.disys.axis.lease.codec;

import java.nio.ByteBuffer;

public class LeaseDataEncoder {
    public byte[] encodeKey(long id) {
        return ByteBuffer.allocate(Long.BYTES).putLong(id).array();
    }

    public byte[] encodeRecord(long ttl, long remainingTtl) {
        return ByteBuffer.allocate(Long.BYTES * 2)
                .putLong(ttl)
                .putLong(remainingTtl)
                .array();
    }

    public byte[] encodeVal(byte[] val, long id) {
        return ByteBuffer.allocate(val.length + Long.BYTES)
                .put(val)
                .putLong(id)
                .array();
    }
}
