package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;

/**
 * Byte sizes and marker values for the binary encoding of {@link Revision}
 * and {@link Record}.
 *
 * <h2>Revision layout</h2>
 * <pre>
 *   [ commitSeq : 8 bytes ][ ordinal : 4 bytes ]
 * </pre>
 *
 * <h2>Record layout</h2>
 * <pre>
 *   [ tombstone : 1 byte ]
 *   [ keyLen : 4 bytes ]
 *   [ key : keyLen bytes ]
 *   [ val : remaining bytes (0 for tombstones) ]
 *   [ version : 4 bytes ]
 *   [ createdAtSeq : 8 bytes ]
 *   [ modifiedAtSeq : 8 bytes ]
 * </pre>
 */
public class CodecConstants {

    /** Byte size of an encoded {@code commitSeq} ({@link Long#BYTES}). */
    public static final int COMMIT_SEQ_SIZE = Long.BYTES;

    /** Byte size of an encoded {@link Revision}: {@code commitSeq} (8) + {@code ordinal} (4). */
    public static final int REVISION_SIZE = COMMIT_SEQ_SIZE + Integer.BYTES;

    /** Encoded form of {@code Revision(0, 0)}: all-zero bytes representing the minimum revision. */
    public static final byte[] START_REVISION = new byte[REVISION_SIZE];

    /** Byte size of the tombstone marker field. */
    static final int TOMBSTONE_MARKER_SIZE = Byte.BYTES;

    /** Byte size of the record metadata fields: {@code version} (4) + {@code createdAtSeq} (8) + {@code modifiedAtSeq} (8). */
    static final int METADATA_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES;

    /** Byte size of the key length prefix field. */
    static final int KEY_LEN_SIZE = Integer.BYTES;

    /** Total fixed overhead per encoded record, excluding key and value bytes. */
    static final int RECORD_OVERHEAD = TOMBSTONE_MARKER_SIZE + METADATA_SIZE + KEY_LEN_SIZE;

    /** Tombstone marker byte value for a live record. */
    static final byte NO_TOMBSTONE = 0;

    /** Tombstone marker byte value for a deleted record. */
    static final byte TOMBSTONE = 1;
}
