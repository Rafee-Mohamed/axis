package io.disys.axis.wal.recovery;

import io.disys.axis.wal.api.Wal;
import io.disys.axis.wal.api.WalConfig;
import io.disys.axis.wal.codec.DecodeResult;
import io.disys.axis.wal.segment.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class Recovery {
    private SegmentView current;
    private final Iterator<Path> segments;
    private final List<Path> paths;
    private final WalConfig config;

    private Recovery(List<Path> paths, WalConfig config) {
        this.segments = paths.iterator();
        this.paths = paths;
        this.config = config;
        this.current = null;
    }

    public static Recovery from(WalConfig config) throws IOException {
        var segments = new ArrayList<Path>();

        try (var filePaths = Files.newDirectoryStream(config.directory())) {
            for (var path: filePaths) {
                segments.add(path);
            }
        }

        var sortedSegments = segments.stream()
                .sorted(Comparator.comparingInt(p -> Integer.parseInt(p.getFileName().toString().split("-")[0])))
                .collect(Collectors.toCollection(ArrayList::new));

        return new Recovery(sortedSegments, config);
    }

    private void drainView(SegmentView view) throws IOException {
        while (view.next() != null) {}
    }

    private void drainSegments() throws IOException {
        var view = next();

        while (view != null) {
            drainView(view);
            view = next();
        }
    }

    public SegmentView next() throws IOException {
        if (!segments.hasNext() && current == null) {
            return null;
        }

        if (!segments.hasNext()) {
            drainView(current);
            return null;
        }

        if (current == null) {
            return current = new SegmentView(ReadableSegment.open(segments.next(), new CRC32()));
        }

        drainView(current);
        if (current.result() instanceof DecodeResult.EndOfSegment(var finalCrc)) {
            current.close();
            return current = new SegmentView(ReadableSegment.open(segments.next(), nextCrc(finalCrc)));
        }

        return null;
    }

    public CRC32 nextCrc(int crcVal) {
        var crc = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        buf.putInt(crcVal);
        buf.flip();
        crc.update(buf);
        return crc;
    }


    public Wal finish() throws IOException {
        drainSegments();
        if (current == null) {
            var firstSegment = WritableSegment.create(
                    0,
                    config.initialIndex(),
                    config,
                    new CRC32()
            );
            var manager = SegmentManager.open(
                    new ArrayList<>(),
                    firstSegment,
                    config
            );
            return Wal.open(manager, config);
        }

        current.close();

        if (segments.hasNext()) {
            throw new IllegalStateException("all segments should be valid except the last one");
        }


        var acitvePath = paths.removeLast();

        var active = switch (current.result()) {
            case DecodeResult.EndOfSegment(var finalCrc) -> WritableSegment.closed(acitvePath, config, nextCrc(finalCrc));
            case DecodeResult.Corrupt(var position, var lastCrc) ->  WritableSegment.open(acitvePath, position, config, nextCrc(lastCrc));
            case DecodeResult.EndOfLog(var position, var lastCrc) -> WritableSegment.open(acitvePath, position, config, nextCrc(lastCrc));
            case DecodeResult.Record _,
                 DecodeResult.Closed _ -> throw new IllegalStateException("invalid state after draining");
        };

        var sealedSegments = paths.stream().map(SealedSegment::from).collect(Collectors.toCollection(ArrayList::new));
        var manager = SegmentManager.open(sealedSegments, active, config);
        return Wal.open(manager, config);
    }
}
