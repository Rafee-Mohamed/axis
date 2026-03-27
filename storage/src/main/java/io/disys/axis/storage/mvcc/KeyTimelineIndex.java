package io.disys.axis.storage.mvcc;

import java.util.*;

public class KeyTimelineIndex {
    private final SortedMap<byte[], KeyTimeline> index;

    public KeyTimelineIndex() {
        this.index = new TreeMap<>(Arrays::compare);
    }

    Optional<KeyTimeline> get(byte[] key) {
        return Optional.ofNullable(index.get(key));
    }

    KeyTimeline add(byte[] key, Revision revision) {
        return index.compute(key, (_, timeline) -> {
            if (timeline == null) {
                return KeyTimeline.init(revision);
            }
            timeline.add(revision);
            return timeline;
        });
    }

    Optional<KeyTimeline> complete(byte[] key, Revision revision) {
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
