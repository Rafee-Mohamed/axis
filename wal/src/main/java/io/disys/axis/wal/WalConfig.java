package io.disys.axis.wal;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import static io.disys.axis.wal.WalConstants.*;

public record WalConfig(
        long segmentSize,
        Path directory,
        long initialIndex,
        IndexStrategy indexer,
        ByteBuffer segmentHeader,
        long maxRecordSize
) implements SegmentConfig {

    public static final class Builder {
        private long segmentSize = DEFAULT_SEGMENT_SIZE;
        private Path directory;
        private long initialIndex = DEFAULT_INITIAL_INDEX;
        private IndexStrategy indexer = new SequentialStrategy();
        private ByteBuffer segmentHeader = ByteBuffer.allocate(0);
        private Long maxRecordSize;

        public Builder segmentSize(long segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder directory(Path directory) {
            this.directory = directory;
            return this;
        }

        public Builder initialIndex(long initialIndex) {
            this.initialIndex = initialIndex;
            return this;
        }

        public Builder indexer(IndexStrategy indexer) {
            this.indexer = indexer;
            return this;
        }

        public Builder segmentHeader(ByteBuffer segmentHeader) {
            this.segmentHeader = segmentHeader;
            return this;
        }

        public Builder maxRecordSize(long maxRecordSize) {
            this.maxRecordSize = maxRecordSize;
            return this;
        }

        public WalConfig build() {
            if (directory == null) {
                throw new IllegalStateException("directory is required");
            }

            var segmentHeaderSize = SEGMENT_HEADER_LENGTH_PREFIX + segmentHeader.remaining();
            var computed = segmentSize
                    - FOOTER_SIZE
                    - segmentHeaderSize
                    - RECORD_HEADER_SIZE
                    - MAX_PADDING;

            if (computed <= 0) {
                throw new IllegalStateException(
                        "segment size too small to hold any records: " + segmentSize);
            }

            var effectiveMaxRecordSize = maxRecordSize != null
                    ? Math.min(maxRecordSize, computed)
                    : computed;

            return new WalConfig(
                    segmentSize,
                    directory,
                    initialIndex,
                    indexer,
                    segmentHeader,
                    effectiveMaxRecordSize
            );
        }
    }
}
