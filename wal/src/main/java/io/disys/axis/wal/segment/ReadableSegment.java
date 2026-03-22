package io.disys.axis.wal.segment;

import io.disys.axis.wal.codec.DecodeResult;
import io.disys.axis.wal.codec.RecordDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import static io.disys.axis.wal.internal.WalConstants.*;

public class ReadableSegment {
    private final RecordDecoder decoder;
    private final FileChannel channel;
    private final ByteBuffer header;

    private ReadableSegment(FileChannel channel, ByteBuffer header, RecordDecoder decoder) {
        this.channel = channel;
        this.header = header;
        this.decoder = decoder;
    }

    private static void readFully(FileChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf);
            if (n < 0) throw new EOFException("unexpected EOF");
        }
    }

    public static ReadableSegment open(Path path, CRC32 crc32) throws IOException {
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        var header = readHeader(channel);
        return new ReadableSegment(channel,header, new RecordDecoder(crc32));
    }

    private static ByteBuffer readHeader(FileChannel channel) throws IOException {
        var lengthBuf = ByteBuffer.allocateDirect(SEGMENT_HEADER_LENGTH_PREFIX);
        readFully(channel, lengthBuf);
        lengthBuf.flip();

        var header = ByteBuffer.allocateDirect(lengthBuf.getInt());
        readFully(channel, header);
        header.flip();

        return header;
    }

    public ByteBuffer header() {
        return header;
    }

    public DecodeResult next() throws IOException {
        if (!isOpen())
            return new DecodeResult.Closed();

        if (channel.position() >= channel.size() - FOOTER_SIZE) {
            return readFooter();
        }

        return decoder.decode(channel);
    }

    private DecodeResult.EndOfSegment readFooter() throws IOException {
        var footer = ByteBuffer.allocateDirect(FOOTER_SIZE);
        readFully(channel, footer);
        footer.flip();
        return new DecodeResult.EndOfSegment(footer.getInt());
    }

    public void close() throws IOException {
        channel.close();
    }

    public boolean isOpen() {
        return channel.isOpen();
    }
}
