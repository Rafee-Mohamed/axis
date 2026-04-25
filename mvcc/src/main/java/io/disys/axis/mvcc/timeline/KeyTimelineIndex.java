package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.codec.RecordDecoder;
import io.disys.axis.mvcc.model.*;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.disys.axis.mvcc.model.Record;
import io.dsal.versioned.index.api.Direction;
import io.dsal.versioned.index.api.OrderedVersionedIndex;
import io.dsal.versioned.index.api.Range;

public class KeyTimelineIndex {
    private final OrderedVersionedIndex<byte[], KeyTimeline> index;
    private final TimelineQuery query;

    public KeyTimelineIndex(OrderedVersionedIndex<byte[], KeyTimeline> index) {
        this.index = index;
        this.query = new TimelineQuery();
    }

    public TimelineTxn txn() {
        return new TimelineTxn(index.txn(), query);
    }

    public TimelineView view() {
        return new TimelineView(index.snapshot(), query);
    }

    public Optional<Revision> revision(byte[] key, Function<KeyTimeline, Optional<Revision>> mapper) {
        return index.get(key).flatMap(mapper);
    }

    public <T> Stream<T> range(byte[] from, byte[] to, BiFunction<byte[], KeyTimeline, Optional<T>> mapper) {
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.iterator(Direction.ASC, Range.closedOpen(from, to)), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv -> mapper.apply(kv.key(), kv.value()))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<KeyRevision> range(byte[] from, byte[] to, Function<KeyTimeline, Optional<Revision>> mapper) {
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.iterator(Direction.ASC, Range.closedOpen(from, to)), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv ->
                        mapper.apply(kv.value())
                                .map(r -> new KeyRevision(kv.key(), r)))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<Revision> revisions(byte[] from, byte[] to, Function<KeyTimeline, Optional<Revision>> mapper) {
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.iterator(Direction.ASC, Range.closedOpen(from, to)), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv -> mapper.apply(kv.value()))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    // Reader read apis - pinned
    public Optional<Revision> pinnedRevisionAt(byte[] key, long commitSeq) {
        return revision(key, t -> t.pinnedRevisionAt(commitSeq));
    }

    public Stream<KeyRevision> pinnedRangeAt(byte[] from, byte[] to, long commitSeq) {
        return range(from, to, t -> t.pinnedRevisionAt(commitSeq));
    }

    public Stream<Revision> pinnedCountAt(byte[] from, byte[] to, long commitSeq) {
        return revisions(from, to, t -> t.pinnedRevisionAt(commitSeq));
    }


    // Writer read apis
    public Optional<Revision> revisionAt(byte[] key, long commitSeq) {
        return revision(key, t -> t.revisionAt(commitSeq));

    }

    // half open - from inclusive, to exclusive
    public Stream<KeyRevision> rangeAt(byte[] from, byte[] to, long commitSeq) {
        return range(from, to, t -> t.revisionAt(commitSeq));
    }

    public Stream<Revision> countAt(byte[] from, byte[] to, long commitSeq) {
        return revisions(from, to, t -> t.revisionAt(commitSeq));
    }

    public void restore(Revision revision, Record record) {
        var existing = index.get(record.key());
        if (existing.isEmpty()) {
            index.put(record.key(), KeyTimeline.restore(revision, record));
        } else if (record.tombstone()) {
            existing.get().tryComplete(revision);
        } else {
            existing.get().add(revision);
        }
    }

    public KeySpan add(byte[] key, Revision revision) {
        var existing = index.get(key);
        if (existing.isEmpty()) {
            var timeline = KeyTimeline.init(revision);
            index.put(key, timeline);
            return timeline.lastSpan();
        }
        var timeline = existing.get();
        timeline.add(revision);
        return timeline.lastSpan();
    }

    public Optional<KeySpan> complete(byte[] key, Revision revision) {
        return index.get(key)
                .filter(t -> t.tryComplete(revision))
                .map(KeyTimeline::lastSpan);
    }

    public Set<Revision> compact(long commitSeq) {
        var retained = new HashSet<Revision>();
        var it = index.snapshot().iterator(Direction.ASC);
        while (it.hasNext()) {
            var entry = it.next();
            var timeline = entry.value().compact(commitSeq);

            if (timeline.isEmpty()) {
                index.remove(entry.key());
                continue;
            }

            var newTl = timeline.get();
            // if same value then don't need to put
            if (newTl != entry.value()) {
                index.put(entry.key(), newTl);
            }

            var firstRevision = newTl.firstRevision();
            // if the first revision is behind the commitSeq
            // then it is preserved for answer queries
            // at or above compact commitSeq, this means
            // everything after firstRevision is strictly
            // greater than commitSeq
            if (firstRevision.compareTo(commitSeq) < 0) {
                retained.add(firstRevision);
            }
        }

        return retained;
    }
}
