package io.disys.axis.wal.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class SegmentManager {
    private final List<SealedSegment> segments;
    private WritableSegment active;
    private final SegmentConfig config;

    SegmentManager(List<SealedSegment> segments, WritableSegment active, SegmentConfig config) {
        this.segments = segments;
        this.active = active;
        this.config = config;
    }

    public static SegmentManager open(List<SealedSegment> segments, WritableSegment active, SegmentConfig config) {
        return new SegmentManager(segments, active, config);
    }


    private void rotate(long index) throws IOException {
        var finalCrc = active.encoder().crc();
        var sealed = active.seal();
        var crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        buf.putInt(finalCrc);
        buf.flip();
        crc.update(buf);

        segments.add(sealed);
        active = WritableSegment.create(segments.size(), index, config, crc);
    }

    private long index(ByteBuffer payload) {
        return config.indexer().index(segments.size(), payload);
    }


    public void append(List<ByteBuffer> payload) throws IOException {
        var unappended = active.append(payload);
        while (!unappended.isEmpty()) {
            rotate(index(unappended.getFirst()));
            unappended = active.append(unappended);
        }
    }

    public void append(ByteBuffer payload) throws IOException {
        if (active.append(payload)) {
            return;
        }
        rotate(index(payload));
        active.append(payload);
    }

    public void truncatePrefix(long index) throws IOException {
        var segmentsToDelete = new ArrayList<SealedSegment>();

        for (var i = 0; i < segments.size() - 1; i++) {
            var next = segments.get(i + 1);
            if (next.index() <= index) {
                segmentsToDelete.add(segments.get(i));
            } else {
                break;
            }
        }

        for (var segment: segmentsToDelete) {
            Files.deleteIfExists(segment.path());
            segments.removeFirst();
        }
    }

    public boolean isOpen() {
        return active != null;
    }

    public void close() throws IOException {
        if (!isOpen()) return;
        segments.add(active.seal());
        active = null;
    }
}
