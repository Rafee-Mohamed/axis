package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.KeyIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class WriterKeyIterator implements KeyIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final long lastCommitSeq;
    private byte[] key;
    private final Predicate<Revision> filter;
    private long remaining;

    public WriterKeyIterator(Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, Predicate<Revision> revisionFilter, long limit) {
        this.timelines = timelines;
        this.key = null;
        this.lastCommitSeq = lastCommitSeq;
        this.filter = revisionFilter;
        this.remaining = limit;
    }

    public WriterKeyIterator(Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, Predicate<Revision> revisionFilter) {
        this(timelines, lastCommitSeq, revisionFilter, Long.MAX_VALUE);
    }

    public WriterKeyIterator(Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, long limit) {
        this(timelines, lastCommitSeq, (_) -> true, limit);
    }

    public WriterKeyIterator(Iterator<KeyTimelineEntry> timelines, long lastCommitSeq) {
        this(timelines, lastCommitSeq, (_) -> true, Long.MAX_VALUE);
    }

    private void updateNext() {
        while (key == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next.timeline()
                    .floor(lastCommitSeq)
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