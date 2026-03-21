import java.nio.file.Path;

public record WalConfig(
        long segmentSize,
        Path directory,
        long initialIndex,
        IndexStrategy indexer
) implements SegmentConfig {
}
