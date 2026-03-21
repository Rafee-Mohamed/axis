import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

public class WritableSegment {

    private static final int PAGE_SIZE = 4096;
    private static final int FOOTER_SIZE = Integer.BYTES;

    private final Path path;
    private final SegmentConfig config;
    private final FileChannel channel;
    private final RecordEncoder encoder;

    private WritableSegment(Path path, FileChannel channel, SegmentConfig config, RecordEncoder encoder) {
        this.path = path;
        this.config = config;
        this.channel = channel;
        this.encoder = encoder;
    }

    public static WritableSegment open(Path segment, long writeFrom, SegmentConfig config, CRC32 crc32) throws IOException {
        var channel = FileChannel.open(segment, StandardOpenOption.WRITE);
        channel.position(writeFrom);
        return new WritableSegment(segment, channel, config, new RecordEncoder(crc32));
    }

    public static WritableSegment closed(Path segment, SegmentConfig config, CRC32 crc32) throws IOException {
        var channel = FileChannel.open(segment);
        channel.close();
        return new WritableSegment(segment, channel, config, new RecordEncoder(crc32));
    }

    private static Path segmentPath(Path dir, int segment, long index) {
        return dir.resolve(segment + "-" + index);
    }

    public static WritableSegment create(int segment, long index, SegmentConfig config, ByteBuffer segmentHeader, CRC32 crc32) throws IOException {
        var path = segmentPath(config.directory(), segment, index);
        var channel = FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        );
        preallocate(channel, config.segmentSize());
        writeHeader(channel, segmentHeader);
        channel.force(true);
        return new WritableSegment(path, channel, config, new RecordEncoder(crc32));
    }

    private static void writeHeader(FileChannel channel, ByteBuffer segmentHeader) throws IOException {
        var headerSize = Integer.BYTES + segmentHeader.remaining();
        var header = ByteBuffer.allocateDirect(headerSize);
        header.putInt(segmentHeader.remaining());
        header.put(segmentHeader.duplicate());
        header.flip();

        channel.write(header);
        channel.force(false);
    }

    private static void preallocate(FileChannel channel, long size) throws IOException {
        var zeros = ByteBuffer.allocateDirect(PAGE_SIZE);
        channel.position(0);

        while (size >= PAGE_SIZE) {
            zeros.clear();
            channel.write(zeros);
            size -= PAGE_SIZE;
        }

        if (size > 0) {
            zeros.clear();
            zeros.limit((int) size);
            channel.write(zeros);
        }

        channel.position(0);
    }

    private boolean exceedsSize(int size) throws IOException {
        return channel.position() + size + FOOTER_SIZE > config.segmentSize();
    }

    public boolean append(ByteBuffer payload) throws IOException {
        if (!isOpen()) return false;

        if (exceedsSize(payload.remaining())) {
            return false;
        }
        encoder.encode(channel, payload);
        channel.force(false);
        return true;
    }

    public List<ByteBuffer> append(List<ByteBuffer> payload) throws IOException {
        if (!isOpen()) return payload;

        var payloadWithinSizeLimit = 0;
        while (payloadWithinSizeLimit < payload.size() && !exceedsSize(payload.get(payloadWithinSizeLimit).remaining())) {
            payloadWithinSizeLimit++;
        }

        var payloadToAppend = payload.subList(0, payloadWithinSizeLimit);
        if (payloadToAppend.isEmpty()) {
            return payload;
        }

        for (var buffer: payloadToAppend) {
            encoder.encode(channel, buffer);
        }

        channel.force(false);

        return payload.subList(payloadWithinSizeLimit, payload.size());
    }

    public SealedSegment seal() throws IOException {
        close();
        return SealedSegment.from(path);
    }

    public void close() throws IOException {
        if (!isOpen()) return;

        var footer = ByteBuffer.allocateDirect(FOOTER_SIZE);
        footer.putInt(encoder.crc());
        footer.flip();
        channel.write(footer);
        channel.force(false);
        channel.close();
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    public RecordEncoder encoder() {
        return encoder;
    }

}
