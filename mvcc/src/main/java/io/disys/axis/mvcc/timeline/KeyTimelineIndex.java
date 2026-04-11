package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.codec.RecordDecoder;
import io.disys.axis.mvcc.model.*;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.disys.axis.mvcc.model.Record;
import io.dsal.persistent.index.core.PersistentBPlusTree;

public class KeyTimelineIndex {
    private final PersistentBPlusTree<byte[], KeyTimeline> index;

    public KeyTimelineIndex(PersistentBPlusTree<byte[], KeyTimeline> index) {
        this.index = index;
    }

    public Optional<Revision> revision(byte[] key, Function<KeyTimeline, Optional<Revision>> mapper) {
        return Optional.ofNullable(index.get(key))
                .flatMap(mapper);
    }

    public <T> Stream<T> range(byte[] from, byte[] to, BiFunction<byte[], KeyTimeline, Optional<T>> mapper) {
        // right now inclusive range for index but sooner index will be changed to half open bounds
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.rangeIterator(from, to), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv -> mapper.apply(kv.key(), kv.val()))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<KeyRevision> range(byte[] from, byte[] to, Function<KeyTimeline, Optional<Revision>> mapper) {
        // right now inclusive range for index but sooner index will be changed to half open bounds
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.rangeIterator(from, to), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv ->
                        mapper.apply(kv.val())
                                .map(r -> new KeyRevision(kv.key(), r)))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    public Stream<Revision> revisions(byte[] from, byte[] to, Function<KeyTimeline, Optional<Revision>> mapper) {
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(index.rangeIterator(from, to), Spliterator.ORDERED),
                false
        );

        return stream
                .map(kv -> mapper.apply(kv.val()))
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
        var timeline = index.get(record.key());

        if (timeline == null) {
            timeline = KeyTimeline.restore(revision, record);
            index.put(record.key(), timeline);
        } else if (record.tombstone()) {
            timeline.tryComplete(revision);
        } else {
            timeline.add(revision);
        }
    }

    public KeySpan add(byte[] key, Revision revision) {
        var timeline = index.get(key);
        if (timeline == null) {
            timeline = KeyTimeline.init(revision);
            index.put(key, timeline);
        } else {
            timeline.add(revision);
        }
        return timeline.lastSpan();
    }

    public Optional<KeySpan> complete(byte[] key, Revision revision) {
        var timeline = index.get(key);
        if (timeline != null && timeline.tryComplete(revision)) {
            return Optional.of(timeline.lastSpan());
        }
        return Optional.empty();
    }

    public Set<Revision> compact(long commitSeq) {
        var retained = new HashSet<Revision>();
        // index iterable iterator pins the current root and
        // walks through that pinned index reading
        // the snapshot as existed during iterator creation
        // therefore, index can be mutated concurrently while iterating
        // while reading from iterator
        for (var entry: index) {
            var timeline = entry.val().compact(commitSeq);

            if (timeline.isEmpty()) {
                index.remove(entry.key());
                continue;
            }

            var newTl = timeline.get();
            // if same value then don't need to put
            if (newTl != entry.val()) {
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
