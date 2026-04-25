package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.SortDirection;
import io.dsal.versioned.index.api.Snapshot;

import java.util.Optional;
import java.util.stream.Stream;

public class TimelineView {
    private final Snapshot<byte[], KeyTimeline> snapshot;
    private final TimelineQuery query;

    public TimelineView(Snapshot<byte[], KeyTimeline> snapshot, TimelineQuery query) {
        this.snapshot = snapshot;
        this.query = query;
    }

    public Optional<RevisionData> getAt(byte[] key, long commitSeq) {
        return query.get(snapshot, key, tl -> tl.getPinnedAt(commitSeq));
    }

    public Stream<KeyRevisionData> rangeAt(byte[] from, byte[] to, long commitSeq, SortDirection direction) {
        return query.range(snapshot, from, to, direction, tl -> tl.getPinnedAt(commitSeq));
    }
}
