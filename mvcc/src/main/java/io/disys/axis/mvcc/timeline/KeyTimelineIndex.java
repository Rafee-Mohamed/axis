package io.disys.axis.mvcc.timeline;

import java.util.*;

import io.dsal.versioned.index.api.OrderedVersionedIndex;

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
}
