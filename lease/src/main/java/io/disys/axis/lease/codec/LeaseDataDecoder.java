package io.disys.axis.lease.codec;

import io.disys.axis.lease.model.LeaseRecord;

import java.nio.ByteBuffer;

public class LeaseDataDecoder {

    public LeaseRecord decodeRecord(byte[] record) {
        var buf = ByteBuffer.wrap(record);
        return new LeaseRecord(buf.getLong(), buf.getLong());
    }

    public long decodeKey(byte[] key) {
        return ByteBuffer.wrap(key).getLong();
    }

    public long decodeLeaseId(byte[] val) {
        return ByteBuffer.wrap(val, val.length - Long.BYTES, Long.BYTES).getLong();
    }
}
