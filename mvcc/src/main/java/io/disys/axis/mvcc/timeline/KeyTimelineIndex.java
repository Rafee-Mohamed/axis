package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.store.ModifiedAtSeqBound;

import javax.swing.text.html.Option;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

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

    public long count(byte[] from, byte[] to,long firstCommitSeq, long lastCommitSeq) {
        return index.subMap(from, to)
                .values()
                .stream()
                .filter(t -> t.pin(firstCommitSeq, lastCommitSeq).isPresent())
                .count();
    }

    public long count(byte[] from, byte[] to,long firstCommitSeq, long lastCommitSeq, Predicate<Revision> filter) {
        return index.subMap(from, to)
                .values()
                .stream()
                .filter(t -> t.pin(firstCommitSeq, lastCommitSeq)
                        .filter(v -> filter.test(v.floor()))
                        .isPresent())
                .count();
    }

    public long count(byte[] from, byte[] to, long lastCommitSeq) {
        return index.subMap(from, to)
                .values()
                .stream()
                .filter(t -> t.floor(lastCommitSeq).isPresent())
                .count();
    }

    public long count(byte[] from, byte[] to, Predicate<Revision> filter) {
        return index.subMap(from, to)
                .values()
                .stream()
                .filter(t -> filter.test(t.floor()))
                .count();
    }

    public long count(byte[] from, byte[] to) {
        return index.subMap(from, to).size();
    }

    public long count(byte[] from, byte[] to, long lastCommitSeq, Predicate<Revision> filter) {
        return index.subMap(from, to)
                .values()
                .stream()
                .filter(t -> t.floor(lastCommitSeq).filter(filter).isPresent())
                .count();
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
        var timeline = index.get(key);
        if (timeline.tryComplete(revision)) {
            return Optional.of(timeline);
        }
        return Optional.empty();
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
