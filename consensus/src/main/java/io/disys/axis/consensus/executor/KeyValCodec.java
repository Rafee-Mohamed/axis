package io.disys.axis.consensus.executor;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.KeyVal;
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

    /**
     * Encodes a value with no lease into the MVCC storage format.
     *
     * @param val the user-visible value
     * @return the encoded bytes ready for storage
     */
    static ByteString encode(ByteString val) {
        return encode(val, 0L);
    }

    /**
     * Encodes a value and lease ID into the MVCC storage format.
     *
     * @param val     the user-visible value
     * @param leaseId the attached lease ID, or {@code 0} if none
     * @return the encoded bytes ready for storage
     */
    static ByteString encode(ByteString val, long leaseId) {
        byte[] prefix = new byte[LEASE_ID_BYTES];
        ByteBuffer.wrap(prefix).putLong(leaseId);
        return ByteString.copyFrom(prefix).concat(val);
    }

    /**
     * Decodes the user-visible value from stored bytes, stripping the lease prefix.
     *
     * @param stored the raw bytes from MVCC
     * @return the user-visible value
     */
    static ByteString decodeVal(byte[] stored) {
        return ByteString.copyFrom(stored, LEASE_ID_BYTES, stored.length - LEASE_ID_BYTES);
    }

    /**
     * Decodes the lease ID from stored bytes.
     *
     * @param stored the raw bytes from MVCC
     * @return the lease ID, or {@code 0} if no lease is attached
     */
    static long decodeLeaseId(byte[] stored) {
        return ByteBuffer.wrap(stored).getLong();
    }

    /**
     * Builds a {@link KeyVal} proto from an MVCC {@link Record},
     * decoding the stored value and lease ID from the record's encoded bytes.
     *
     * @param record the MVCC record
     * @return the decoded {@link KeyVal} proto
     */
    static KeyVal toKeyVal(Record record) {
        return KeyVal.newBuilder()
                .setKey(ByteString.copyFrom(record.key()))
                .setVal(decodeVal(record.val()))
                .setLeaseId(decodeLeaseId(record.val()))
                .setVersion(record.version())
                .setCreatedRevision(record.createdAtSeq())
                .setModifiedRevision(record.modifiedAtSeq())
                .build();
    }
}
