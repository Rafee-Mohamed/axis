import java.nio.file.Path;

public record WalConfig(
        long segmentSize,
        long maxRecordSize,
        Path directory
) implements SegmentConfig {
}
