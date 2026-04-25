package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.model.SortDirection;
import io.dsal.versioned.index.api.Direction;
import io.dsal.versioned.index.api.Txn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class TimelineTxn {
    private final Txn<byte[], KeyTimeline> txn;
    private final TimelineQuery query;

    public TimelineTxn(Txn<byte[], KeyTimeline> txn, TimelineQuery query) {
        this.txn = txn;
        this.query = query;
    }

    public Optional<RevisionData> getAt(byte[] key, long commitSeq) {
        return query.get(txn, key, tl -> tl.getAt(commitSeq));
    }

    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq, SortDirection direction) {
        return query.range(txn, from, to, direction,  tl -> tl.getAt(commitSeq));
    }

    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq) {
        return query.range(txn, from, to, SortDirection.ASCENDING,  tl -> tl.getAt(commitSeq));
    }

    public <T> Stream<T> range(byte[] from, byte[] to, BiFunction<byte[], KeyTimeline, Optional<T>> mapper) {
        return query.range(txn, from, to, SortDirection.ASCENDING, mapper);
    }

    public void restore(Revision revision, Record record) {
        var existing = txn.get(record.key());
        if (existing.isEmpty()) {
            txn.put(record.key(), KeyTimeline.restore(revision, record));
        } else if (record.tombstone()) {
            existing.get().tryComplete(revision);
        } else {
            existing.get().add(revision);
        }
    }

    public KeySpan add(byte[] key, Revision revision) {
        return txn.get(key).map(tl -> {
            tl.add(revision);
            return tl;
        }).orElseGet(() -> {
            var tl = KeyTimeline.init(revision);
            txn.put(key, tl);
            return tl;
        }).lastSpan();
    }


    public Optional<KeySpan> complete(byte[] key, Revision revision) {
        return txn.get(key)
                .filter(t -> t.tryComplete(revision))
                .map(KeyTimeline::lastSpan);
    }


    public Set<Revision> compact(long commitSeq) {
        var retained = new HashSet<Revision>();

        txn.snapshot().forEach(Direction.ASC, (key, tl) -> {
            var compacted = tl.compact(commitSeq);

            if (compacted.isEmpty()) {
                txn.remove(key);
                return;
            }

            var newTl = compacted.get();
            // if same value then don't need to put
            if (newTl != tl) {
                txn.put(key, newTl);
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
        });

        return retained;
    }

    public void commit() {
        txn.commit();
    }

}
