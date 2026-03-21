import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

public class Wal {
    private final SegmentManager manager;
    private final WalConfig config;

    public Wal(SegmentManager manager, WalConfig config) {
        this.manager = manager;
        this.config = config;
    }

    static Recovery recover(WalConfig config, ByteBuffer segmentHeader) throws IOException {
        return Recovery.from(config, segmentHeader);
    }

    static Wal open(SegmentManager manager, WalConfig config) {
        return new Wal(manager, config);
    }

    public void append(ByteBuffer payload) throws IOException {
        manager.append(payload);
    }

    public void append(List<ByteBuffer> payload) throws IOException {
        manager.append(payload);
    }

    public void truncatePrefix(long index) {

    }

    public void close() {

    }
}
