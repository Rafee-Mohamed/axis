import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class SegmentManager {
    private final List<SealedSegment> segments;
    private WritableSegment active;
    private final SegmentConfig config;
    private final ByteBuffer segmentHeader;

    SegmentManager(List<SealedSegment> segments, WritableSegment active, SegmentConfig config, ByteBuffer segmentHeader) {
        this.segments = segments;
        this.active = active;
        this.config = config;
        this.segmentHeader = segmentHeader;
    }

    private static Path segmentPath(Path path, int segment, long index) {
        return path.resolve(segment + "-" + index);
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
        active = WritableSegment.create(segments.size(), index, config, segmentHeader, crc);
    }

    private long index(ByteBuffer payload) {
        return config.indexer().index(segments.size(), payload);
    }


    public void append(List<ByteBuffer> payload) throws IOException {
        var unappended = active.append(payload);
        if (unappended.isEmpty()) {
            return;
        }
        rotate(index(unappended.getFirst()));
        active.append(unappended);
    }

    public void append(ByteBuffer payload) throws IOException {
        if (active.append(payload)) {
            return;
        }
        rotate(index(payload));
        active.append(payload);
    }
}
