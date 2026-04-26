package io.disys.axis.mvcc.timeline;

import java.util.*;

import io.dsal.versioned.index.api.OrderedVersionedIndex;

/**
 * An ordered index mapping each user key ({@code byte[]}) to its complete revision
 * history ({@link KeyTimeline}). Together the entries cover the full MVCC state of
 * the store: for any key and any commitSeq, the timeline answers what value the key
 * held at that point in time.
 *
 * <p>The ordering by key supports range queries across the key space. Access is split
 * into two modes: a single writer via {@link #txn()} and any number of concurrent
 * snapshot-isolated readers via {@link #view()}.</p>
 */
public class KeyTimelineIndex {
    /** Underlying ordered versioned index storing {@link KeyTimeline} values keyed by byte arrays. */
    private final OrderedVersionedIndex<byte[], KeyTimeline> index;

    /** Shared stateless query utilities; safe to reuse across transactions and views. */
    private final TimelineQuery query;

    public KeyTimelineIndex(OrderedVersionedIndex<byte[], KeyTimeline> index) {
        this.index = index;
        this.query = new TimelineQuery();
    }

    /** @return a new writer transaction; only one should be active at a time */
    public TimelineTxn txn() {
        return new TimelineTxn(index.txn(), query);
    }

    /**
     * @return a snapshot-isolated read view over the current committed state;
     *         multiple views can be active concurrently
     */
    public TimelineView view() {
        return new TimelineView(index.snapshot(), query);
    }
}
