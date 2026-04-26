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

/**
 * Writer-side transaction context over the timeline index. Not thread-safe;
 * only one instance should be active at a time.
 *
 * <p>Reads through this class observe the transaction's current working state,
 * including uncommitted mutations. For stable concurrent reads unaffected by
 * in-flight mutations, use {@link TimelineView} instead.</p>
 */
public class TimelineTxn {
    /** Underlying index transaction; accumulates mutations until {@link #commit()}. */
    private final Txn<byte[], KeyTimeline> txn;
    private final TimelineQuery query;

    public TimelineTxn(Txn<byte[], KeyTimeline> txn, TimelineQuery query) {
        this.txn = txn;
        this.query = query;
    }

    /** @return the revision of {@code key} visible at {@code commitSeq}, or empty if absent or deleted */
    public Optional<RevisionData> getAt(byte[] key, long commitSeq) {
        return query.get(txn, key, tl -> tl.getAt(commitSeq));
    }

    /** Streams key revisions in {@code [from, to)} visible at {@code commitSeq} in {@code direction}. */
    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq, SortDirection direction) {
        return query.range(txn, from, to, direction,  tl -> tl.getAt(commitSeq));
    }

    /** Ascending convenience overload of {@link #rangeAt(byte[], byte[], long, SortDirection)}. */
    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq) {
        return query.range(txn, from, to, SortDirection.ASCENDING,  tl -> tl.getAt(commitSeq));
    }

    /** Streams results over {@code [from, to)} in ascending order, applying {@code mapper} to each timeline. */
    public <T> Stream<T> range(byte[] from, byte[] to, BiFunction<byte[], KeyTimeline, Optional<T>> mapper) {
        return query.range(txn, from, to, SortDirection.ASCENDING, mapper);
    }

    /**
     * Rebuilds a timeline entry from persisted state. Creates a new timeline on first
     * encounter; appends to an existing one for subsequent revisions of the same key.
     */
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

    /**
     * Appends {@code revision} to the timeline for {@code key}, creating it if absent.
     *
     * @return the last span of the timeline after the append
     */
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

    /**
     * Closes the live span of {@code key}'s timeline with {@code revision} as the deletion.
     *
     * @return the resulting dead span, or empty if the key does not exist or is already deleted
     */
    public Optional<KeySpan> complete(byte[] key, Revision revision) {
        return txn.get(key)
                .filter(t -> t.tryComplete(revision))
                .map(KeyTimeline::lastSpan);
    }

    /**
     * Compacts all timelines to {@code commitSeq} and returns the set of retained revisions.
     * Iterates over {@code txn.snapshot()} — the immutable committed state at transaction start —
     * because the index mutates nodes in place; iterating the working state while mutating it
     * via the same txn is undefined behavior. Compaction must be the sole operation in its txn.
     *
     * @param commitSeq  the compaction boundary
     * @return revisions that must be retained in the backend store to serve queries at {@code commitSeq}
     */
    public Set<Revision> compact(long commitSeq) {
        var retained = new HashSet<Revision>();

        txn.snapshot().forEach(Direction.ASC, (key, tl) -> {
            var compacted = tl.compact(commitSeq);

            if (compacted.isEmpty()) {
                txn.remove(key);
                return;
            }

            var newTl = compacted.get();
            // reference equality: tl.compact() returns the same instance when nothing was trimmed;
            // no need to put it back into the index
            if (newTl != tl) {
                txn.put(key, newTl);
            }

            var firstRevision = newTl.firstRevision();
            // first revision is strictly before commitSeq — it is the floor revision preserved
            // to answer queries at exactly commitSeq; everything else is after the compaction point
            if (firstRevision.compareTo(commitSeq) < 0) {
                retained.add(firstRevision);
            }
        });

        return retained;
    }

    /** Commits all accumulated mutations atomically. */
    public void commit() {
        txn.commit();
    }

}
