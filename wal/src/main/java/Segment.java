import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32;

public class Segment {

    private static final int PAGE_SIZE = 4096;
    private static final ByteBuffer ZEROS = ByteBuffer.allocateDirect(PAGE_SIZE);
    private static final int FOOTER_SIZE = Integer.BYTES;

    private final SegmentConfig config;
    private final FileChannel channel;
    private final RecordEncoder encoder;

    private Segment(FileChannel channel, SegmentConfig config, RecordEncoder encoder) {
        this.config = config;
        this.channel = channel;
        this.encoder = encoder;
    }

    public static Segment open(Path segment, SegmentConfig config, CRC32 crc32) throws IOException {
        var channel = FileChannel.open(segment);
        channel.position(0);
        return new Segment(channel, config, new RecordEncoder(crc32));
    }

    public static Segment create(Path segment, SegmentConfig config, byte[] segmentHeader, CRC32 crc32) throws IOException {
        var channel = getSizeAllocatedFile(Files.createFile(segment), config);
        writeHeader(channel, segmentHeader);
        return new Segment(channel, config, new RecordEncoder(crc32));
    }

    private static void writeHeader(FileChannel channel, byte[] segmentHeader) throws IOException {
        var headerSize = Integer.BYTES + segmentHeader.length;
        var header = ByteBuffer.allocateDirect(headerSize);
        header.putInt(segmentHeader.length);
        header.put(segmentHeader);
        header.flip();
        channel.write(header);
        channel.force(false);
    }

    private static FileChannel getSizeAllocatedFile(Path path, SegmentConfig config) throws IOException {
        var channel = FileChannel.open(path);
        channel.position(0);

        var remaining = config.segmentSize();

        while (remaining >= PAGE_SIZE) {
            ZEROS.clear();
            channel.write(ZEROS);
            remaining -= PAGE_SIZE;
        }

        if (remaining > 0) {
            ZEROS.clear();
            ZEROS.limit((int) remaining);
            channel.write(ZEROS);
        }

        ZEROS.clear();
        channel.position(0);
        channel.force(true);

        return channel;
    }

    private boolean exceedsSize(int size) throws IOException {
        return channel.position() + size + FOOTER_SIZE > config.segmentSize();
    }

    public boolean append(ByteBuffer payload) throws IOException {
        if (exceedsSize(payload.capacity())) {
            return false;
        }
        channel.write(encoder.encode(payload));
        channel.force(false);
        return true;
    }

    public List<ByteBuffer> append(List<ByteBuffer> payload) throws IOException {
        var payloadWithinSizeLimit = 0;
        while (!exceedsSize(payload.get(payloadWithinSizeLimit).capacity())) {
            payloadWithinSizeLimit++;
        }

        var payloadToAppend = payload.subList(0, payloadWithinSizeLimit);
        if (payloadToAppend.isEmpty()) {
            return payload;
        }

        for (var buffer: payload) {
            channel.write(encoder.encode(buffer));
        }

        channel.force(false);

        return payload.subList(payloadWithinSizeLimit, payload.size());
    }

    public void close() throws IOException {
        var footer = ByteBuffer.allocateDirect(FOOTER_SIZE);
        footer.putInt(encoder.crc());
        channel.write(footer);
        channel.force(false);
        channel.close();
    }

    public RecordEncoder encoder() {
        return encoder;
    }

}
