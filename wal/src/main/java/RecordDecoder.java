import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

public class RecordDecoder {
    private final CRC32 crc;
    private final ByteBuffer header;

    public RecordDecoder(CRC32 crc) {
        this.crc = crc;
        this.header = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES);
    }

    public DecodeResult decode(FileChannel channel) throws IOException {
        header.clear();

        channel.read(header);
        header.flip();

        var lengthAndPadding = header.getLong();
        if (lengthAndPadding == 0) {
            channel.position(channel.position() - header.capacity());
            return new DecodeResult.EndOfLog(channel.position(), (int) crc.getValue());
        }

        var checksum = header.getInt();

        var length = (int) (lengthAndPadding >> 8);
        var padding = lengthAndPadding & 0xFF;

        var payload = ByteBuffer.allocateDirect(length);
        channel.read(payload);
        payload.flip();

        var previousCrc = (int) crc.getValue();
        crc.update(payload.duplicate());
        var currCrc = (int) crc.getValue();
        if (currCrc != checksum) {
            channel.position(channel.position() - header.capacity() - length);
            return new DecodeResult.Corrupt(channel.position(), previousCrc);
        }

        channel.position(channel.position() + padding);

        return new DecodeResult.Record(payload);
    }

}
