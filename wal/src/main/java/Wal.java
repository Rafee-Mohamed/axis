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

    static Wal open(Path directory, WalConfig config, byte[] segmentHeader) throws IOException {
        var manager = SegmentManager.open(config, segmentHeader);
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
