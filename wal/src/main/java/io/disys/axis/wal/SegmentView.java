package io.disys.axis.wal;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SegmentView implements AutoCloseable {
    private final ReadableSegment segment;
    private DecodeResult result;

    public SegmentView(ReadableSegment segment) {
        this.segment = segment;
        this.result = null;
    }

    public ByteBuffer header() {
        return segment.header();
    }

    public ByteBuffer next() throws IOException {
        if (!segment.isOpen() || result != null) {
            return null;
        }
        var next = segment.next();
        if (next instanceof DecodeResult.Record(var payload)) {
            return payload;
        }

        result = next;
        return null;
    }

    DecodeResult result() {
        return result;
    }

    @Override
    public void close() throws IOException {
        segment.close();
    }
}
