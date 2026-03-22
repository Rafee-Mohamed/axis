package io.disys.axis.wal.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import static io.disys.axis.wal.internal.WalConstants.*;

public class RecordEncoder {
    private final CRC32 crc;
    // CRC + payload length
    private final ByteBuffer header;
    private final ByteBuffer padding;

    public RecordEncoder(CRC32 crc) {
        this.crc = crc;
        this.padding = ByteBuffer.allocateDirect(MAX_PADDING);
        this.header = ByteBuffer.allocateDirect(RECORD_HEADER_SIZE);
    }

    public void encode(FileChannel channel, ByteBuffer payload) throws IOException {
        var payloadLength = payload.remaining();
        var recordLength = payloadLength + header.capacity();
        var paddingLen = (8 - (recordLength % 8)) % 8;
        var length = ((long) payloadLength << 8) | paddingLen;

        crc.update(payload.duplicate());

        header.clear();
        header.putLong(length);
        header.putInt(crc());
        header.flip();
        channel.write(header);

        channel.write(payload);

        if (paddingLen > 0) {
            padding.clear();
            padding.limit(paddingLen);
            channel.write(padding);
        }
    }

    public int crc() {
        return (int) crc.getValue();
    }
}
