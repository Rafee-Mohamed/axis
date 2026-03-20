import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class SegmentManager {
    private final List<Segment> segments;
    private final SegmentConfig config;
    private final byte[] segmentHeader;

    private SegmentManager(List<Segment> segments, SegmentConfig config, byte[] segmentHeader) {
        this.segments = segments;
        this.config = config;
        this.segmentHeader = segmentHeader;
    }

    public static SegmentManager open(SegmentConfig config, byte[] segmentHeader) throws IOException {
        var segments = new ArrayList<Segment>();
        try (var filePaths = Files.newDirectoryStream(config.directory())) {
            for (var path: filePaths) {
                segments.add(Segment.open(path, config, new CRC32()));
            }
        }
        return new SegmentManager(segments, config, segmentHeader);
    }

    private void rotate() throws IOException {
        var active = active();
        active.close();
        var crc = new CRC32();
        crc.update(active.encoder().crc());
        var path = segments.size() + " " + 0;
        var nextSegment = Segment.create(config.directory().resolve(path), config, segmentHeader, crc);
        segments.add(nextSegment);
    }

    private Segment active() {
        return segments.getLast();
    }

    public void append(List<ByteBuffer> payload) throws IOException {
        var unappended = active().append(payload);
        if (unappended.isEmpty()) {
            return;
        }
        rotate();
        active().append(payload);
    }

    public void append(ByteBuffer payload) throws IOException {
        if (active().append(payload)) {
            return;
        }
        rotate();
        active().append(payload);
    }
}
