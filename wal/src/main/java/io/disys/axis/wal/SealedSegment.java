package io.disys.axis.wal;

import java.nio.file.Path;

public class SealedSegment {
    private final Path path;
    private final long index;
    private final int segment;

    private SealedSegment(Path path, int segment, long index) {
        this.path = path;
        this.segment = segment;
        this.index = index;
    }

    public static SealedSegment from(Path path) {
        var file = path.getFileName().toString();
        var id = file.split("-");
        var segment = Integer.parseInt(id[0]);
        var index = Long.parseLong(id[1]);
        return new SealedSegment(path, segment, index);
    }

    public Path path() { return path; }

    public int segment() { return segment; }

    public long index() { return index; }
}
