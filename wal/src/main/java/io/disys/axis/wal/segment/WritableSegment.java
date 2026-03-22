package io.disys.axis.wal.segment;

import io.disys.axis.wal.codec.RecordEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;
import static io.disys.axis.wal.internal.WalConstants.*;

public class WritableSegment {

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

    public static WritableSegment create(int segment, long index, SegmentConfig config, CRC32 crc32) throws IOException {
        var path = segmentPath(config.directory(), segment, index);
        var channel = FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        );
        preallocate(channel, config.segmentSize());
        writeHeader(channel, config.segmentHeader());
        channel.force(true);
        return new WritableSegment(path, channel, config, new RecordEncoder(crc32));
    }

    private static void writeHeader(FileChannel channel, ByteBuffer segmentHeader) throws IOException {
        var headerSize = Integer.BYTES + segmentHeader.remaining();
        var header = ByteBuffer.allocateDirect(headerSize);
        header.putInt(segmentHeader.remaining());
        header.put(segmentHeader.duplicate());
        header.flip();

        writeFully(channel, header);
        channel.force(false);
    }

    private static void preallocate(FileChannel channel, long size) throws IOException {
        var zeros = ByteBuffer.allocateDirect(PAGE_SIZE);
        channel.position(0);

        while (size >= PAGE_SIZE) {
            zeros.clear();
            writeFully(channel, zeros);
            size -= PAGE_SIZE;
        }

        if (size > 0) {
            zeros.clear();
            zeros.limit((int) size);
            writeFully(channel, zeros);
        }

        channel.position(0);
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private boolean exceedsSize(long position, int payloadSize) throws IOException {
        return position + payloadSize + RECORD_OVERHEAD + FOOTER_SIZE > config.segmentSize();
    }

    private long recordSize(int payloadSize) {
        return RECORD_OVERHEAD + payloadSize;
    }

    public boolean append(ByteBuffer payload) throws IOException {
        if (!isOpen()) return false;

        if (exceedsSize(channel.position(), payload.remaining())) {
            return false;
        }
        encoder.encode(channel, payload);
        channel.force(false);
        return true;
    }

    private int payloadWithinSizeLimit(List<ByteBuffer> payload) throws IOException {
        var payloadWithinSizeLimit = 0;
        var projectedPosition = channel.position();
        while (payloadWithinSizeLimit < payload.size()) {
            var payloadSize = payload.get(payloadWithinSizeLimit).remaining();
            var recordSize = recordSize(payloadSize);
            if (exceedsSize(projectedPosition, payloadSize)) {
                break;
            }
            projectedPosition += recordSize;
            payloadWithinSizeLimit++;
        }

        return payloadWithinSizeLimit;
    }

    public List<ByteBuffer> append(List<ByteBuffer> payload) throws IOException {
        if (!isOpen()) return payload;

        var payloadWithinSizeLimit = payloadWithinSizeLimit(payload);

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
        writeFully(channel, footer);
        // truncate any extra space
        channel.truncate(channel.position());
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
