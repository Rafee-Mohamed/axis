import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public class ReadableSegment {
    private final RecordDecoder decoder;
    private final FileChannel channel;
    private final ByteBuffer header;
    private static final int FOOTER_SIZE = Integer.BYTES;

    private ReadableSegment(FileChannel channel, ByteBuffer header, RecordDecoder decoder) {
        this.channel = channel;
        this.header = header;
        this.decoder = decoder;
    }

    public static ReadableSegment open(Path path, CRC32 crc32) throws IOException {
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        var header = readHeader(channel);
        return new ReadableSegment(channel,header, new RecordDecoder(crc32));
    }

    private static ByteBuffer readHeader(FileChannel channel) throws IOException {
        var lengthBuf = ByteBuffer.allocateDirect(Integer.BYTES);
        channel.read(lengthBuf);
        lengthBuf.flip();
        var header = ByteBuffer.allocateDirect(lengthBuf.getInt());
        channel.read(header);
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
        channel.read(footer);
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
