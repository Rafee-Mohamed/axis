package io.disys.axis.consensus.sm;

import com.google.protobuf.ByteString;
import io.disys.axis.mvcc.model.Record;

import java.nio.ByteBuffer;

/**
 * Encodes and decodes values stored in MVCC.
 *
 * <h2>Storage Layout</h2>
 * <pre>
 * | 8 bytes leaseId | actual value bytes |
 *        0..7             8..len
 * </pre>
 *
 * <p>A {@code leaseId} of {@code 0} means no lease is attached.
 * The lease field is always present — keys without a lease store
 * eight zero bytes in the prefix.</p>
 */
final class KeyValCodec {

    private static final int LEASE_ID_BYTES = 8;

    private KeyValCodec() {}

    static ByteString encode(ByteString val) {
        return encode(val, 0L);
    }

    static ByteString encode(ByteString val, long leaseId) {
        byte[] prefix = new byte[LEASE_ID_BYTES];
        ByteBuffer.wrap(prefix).putLong(leaseId);
        return ByteString.copyFrom(prefix).concat(val);
    }

    static ByteString decodeVal(byte[] stored) {
        return ByteString.copyFrom(stored, LEASE_ID_BYTES, stored.length - LEASE_ID_BYTES);
    }

    static long decodeLeaseId(byte[] stored) {
        return ByteBuffer.wrap(stored).getLong();
    }

    static io.disys.axis.api.proto.KeyVal toKeyVal(Record record) {
        return io.disys.axis.api.proto.KeyVal.newBuilder()
                .setKey(ByteString.copyFrom(record.key()))
                .setVal(decodeVal(record.val()))
                .setLeaseId(decodeLeaseId(record.val()))
                .setVersion(record.version())
                .setCreatedRevision(record.createdAtSeq())
                .setModifiedRevision(record.modifiedAtSeq())
                .build();
    }
}
