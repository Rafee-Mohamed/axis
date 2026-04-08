package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.KeyIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;
import io.disys.axis.mvcc.timeline.KeyTimelineView;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class ReaderKeyIterator implements KeyIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final long firstCommitSeq;
    private final long lastCommitSeq;
    private final Predicate<Revision> filter;
    private byte[] key;
    private long remaining;

    public ReaderKeyIterator(Iterator<KeyTimelineEntry> timelines, long firstCommitSeq, long lastCommitSeq, Predicate<Revision> revisionFilter, long limit) {
        this.timelines = timelines;
        this.key = null;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
        this.filter = revisionFilter;
        this.remaining = limit;
    }

    public ReaderKeyIterator(
            Iterator<KeyTimelineEntry> timelines,
            long firstCommitSeq,
            long lastCommitSeq,
            Predicate<Revision> revisionFilter
    ) {
        this(timelines, firstCommitSeq, lastCommitSeq, revisionFilter, Long.MAX_VALUE);
    }

    public ReaderKeyIterator(Iterator<KeyTimelineEntry> timelines, long firstCommitSeq, long lastCommitSeq, long limit) {
        this(timelines, firstCommitSeq, lastCommitSeq, (_) -> true, limit);
    }

    public ReaderKeyIterator(Iterator<KeyTimelineEntry> timelines, long firstCommitSeq, long lastCommitSeq) {
        this(timelines, firstCommitSeq, lastCommitSeq, (_) -> true, Long.MAX_VALUE);
    }

    private void updateNext() {
        while (key == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next
                    .timeline()
                    .pin(firstCommitSeq, lastCommitSeq)
                    .map(KeyTimelineView::floor)
                    .filter(filter)
                    .map(_ -> next.key())
                    .orElse(null);
        }
    }

    @Override
    public boolean hasNext() {
        if (remaining <= 0) {
            return false;
        }
        updateNext();
        return key != null;
    }

    @Override
    public byte[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        var next = key;
        key = null;
        remaining--;

        return next;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return this;
    }
}
