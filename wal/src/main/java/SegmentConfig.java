import java.nio.file.Path;

public interface SegmentConfig {
    long segmentSize();
    Path directory();
}
