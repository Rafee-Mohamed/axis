package io.disys.axis.wal;

import java.nio.ByteBuffer;

public class ExtractStrategy implements IndexStrategy {
    private final IndexExtractor extractor;

    public ExtractStrategy(IndexExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public long index(long seq, ByteBuffer payload) {
        return extractor.extract(payload);
    }
}
