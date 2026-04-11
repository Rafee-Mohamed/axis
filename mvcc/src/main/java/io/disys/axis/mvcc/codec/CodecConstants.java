package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;

public class CodecConstants {
    public static final int REVISION_SIZE = Long.BYTES + Integer.BYTES;
    public static final byte[] START_REVISION = new byte[REVISION_SIZE];
    static final int TOMBSTONE_MARKER_SIZE = Byte.BYTES;
    static final int METADATA_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES;
    static final int KEY_LEN_SIZE = Integer.BYTES;
    static final int RECORD_OVERHEAD = TOMBSTONE_MARKER_SIZE + METADATA_SIZE + KEY_LEN_SIZE;
    static final byte NO_TOMBSTONE = 0;
    static final byte TOMBSTONE = 1;
}
