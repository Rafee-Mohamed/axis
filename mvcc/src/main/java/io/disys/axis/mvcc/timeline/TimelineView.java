package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.SortDirection;
import io.dsal.versioned.index.api.Snapshot;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * A point-in-time read view over the timeline index. Combines a {@link Snapshot}
 * (which fixes the set of visible keys at index snapshot time) with a {@code commitSeq}
 * per query (which selects the revision within each key's timeline visible at that point).
 */
public class TimelineView {
    /** Immutable view of the index's committed key set; unaffected by subsequent commits. */
    private final Snapshot<byte[], KeyTimeline> snapshot;
    private final TimelineQuery query;

    public TimelineView(Snapshot<byte[], KeyTimeline> snapshot, TimelineQuery query) {
        this.snapshot = snapshot;
        this.query = query;
    }

    /** @return the revision of {@code key} visible at {@code commitSeq}, or empty if absent or deleted */
    public Optional<RevisionData> getAt(byte[] key, long commitSeq) {
        return query.get(snapshot, key, tl -> tl.getPinnedAt(commitSeq));
    }

    /** Streams key revisions in {@code [from, to)} visible at {@code commitSeq} in {@code direction}. */
    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq, SortDirection direction) {
        return query.range(snapshot, from, to, direction, tl -> tl.getPinnedAt(commitSeq));
    }

    /** Ascending convenience overload of {@link #rangeAt(byte[], byte[], long, SortDirection)}. */
    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq) {
        return query.range(snapshot, from, to, SortDirection.ASCENDING, tl -> tl.getPinnedAt(commitSeq));
    }
}
