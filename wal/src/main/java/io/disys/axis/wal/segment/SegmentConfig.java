package io.disys.axis.wal.segment;

import io.disys.axis.wal.index.IndexStrategy;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface SegmentConfig {
    long segmentSize();
    Path directory();
    IndexStrategy indexer();
    long initialIndex();
    ByteBuffer segmentHeader();
}
