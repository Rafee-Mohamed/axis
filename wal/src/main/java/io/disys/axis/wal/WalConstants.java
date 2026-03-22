package io.disys.axis.wal;

public final class WalConstants {
    private WalConstants() {}

    // Record envelope
    public static final int RECORD_HEADER_SIZE = Long.BYTES + Integer.BYTES; // 12 bytes
    public static final int MAX_PADDING = Long.BYTES - 1;                    // 7 bytes
    public static final int RECORD_OVERHEAD = RECORD_HEADER_SIZE + MAX_PADDING;

    // Segment structure
    public static final int FOOTER_SIZE = Integer.BYTES;                     // 4 bytes
    public static final int SEGMENT_HEADER_LENGTH_PREFIX = Integer.BYTES;    // 4 bytes

    // I/O
    public static final int PAGE_SIZE = 4096;

    // Defaults
    public static final long DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024L;      // 64MB
    public static final long DEFAULT_INITIAL_INDEX = 0L;
}