package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;

import java.util.*;

public class KeyTimelineIndex {
    private final SortedMap<byte[], KeyTimeline> index;

    public KeyTimelineIndex() {
        this.index = new TreeMap<>(Arrays::compare);
    }

    public Optional<KeyTimeline> get(byte[] key) {
        return Optional.ofNullable(index.get(key));
    }

    public Optional<Revision> revision(byte[] key, long firstCommitSeq, long lastCommitSeq) {
        return Optional.ofNullable(index.get(key))
                .flatMap(t -> t.pin(firstCommitSeq, lastCommitSeq))
                .map(KeyTimelineView::floor);
    }

    public Optional<Revision> revision(byte[] key) {
        return Optional.ofNullable(index.get(key))
                .map(KeyTimeline::floor);
    }

    public Optional<Revision> revision(byte[] key, long lastCommitSeq) {
        return Optional.ofNullable(index.get(key))
                .flatMap(t -> t.floor(lastCommitSeq));
    }

    public Iterator<KeyTimelineEntry> range(byte[] from, byte[] to) {
        return index.subMap(from, to)
                .entrySet()
                .stream()
                .map(e -> new KeyTimelineEntry(e.getKey(), e.getValue()))
                .iterator();
    }

    public KeyTimeline add(byte[] key, Revision revision) {
        return index.compute(key, (_, timeline) -> {
            if (timeline == null) {
                return KeyTimeline.init(revision);
            }
            timeline.add(revision);
            return timeline;
        });
    }

    public Optional<KeyTimeline> complete(byte[] key, Revision revision) {
        var keyTimeline = index.computeIfPresent(key, (_, timeline) -> {
            timeline.complete(revision);
            return timeline;
        });

        return Optional.ofNullable(keyTimeline);
    }

    public KeyTimelineIndex compact(long commitSeq) {
        var compactedIndex = new KeyTimelineIndex();
        for (var entry: index.entrySet()) {
            var key = entry.getKey();
            var timeline = entry.getValue().compact(commitSeq);
            timeline.ifPresent(keyTimeline -> compactedIndex.index.put(key, keyTimeline));
        }
        return compactedIndex;
    }
}
