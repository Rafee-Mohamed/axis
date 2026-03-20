import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class RecordEncoder {
    private final CRC32 crc;
    private final ByteBuffer recordBuffer;

    public RecordEncoder(CRC32 crc) {
        this.crc = crc;
        this.recordBuffer = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES);
    }

    public ByteBuffer encode(ByteBuffer payload) {
        var length = payload.capacity();
        crc.update(payload);

        recordBuffer.clear();
        recordBuffer.putLong(length);
        recordBuffer.putInt(crc());
        recordBuffer.clear();

        return recordBuffer;
    }

    public int crc() {
        return (int) crc.getValue();
    }
}
