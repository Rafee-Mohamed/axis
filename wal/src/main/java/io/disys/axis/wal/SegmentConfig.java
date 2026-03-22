package io.disys.axis.wal;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface SegmentConfig {
    long segmentSize();
    Path directory();
    IndexStrategy indexer();
    long initialIndex();
    ByteBuffer segmentHeader();
}
