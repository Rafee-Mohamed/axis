package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.RecordIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;
import io.disys.axis.mvcc.timeline.KeyTimelineView;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class ReaderRecordIterator implements RecordIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final CommitBoundedReader reader;
    private final long firstCommitSeq;
    private final long lastCommitSeq;
    private final Predicate<Revision> filter;
    private long remaining;
    private byte[] key;
    private Revision revision;


    public ReaderRecordIterator(
            CommitBoundedReader reader,
            Iterator<KeyTimelineEntry> timelines,
            long firstCommitSeq,
            long lastCommitSeq,
            Predicate<Revision> revisionFilter,
            long limit
    ) {
        this.timelines = timelines;
        this.reader = reader;
        this.key = null;
        this.revision = null;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
        this.filter = revisionFilter;
        this.remaining = limit;
    }


    public ReaderRecordIterator(
            CommitBoundedReader reader,
            Iterator<KeyTimelineEntry> timelines,
            long firstCommitSeq,
            long lastCommitSeq,
            Predicate<Revision> revisionFilter
    ) {
        this(reader, timelines, firstCommitSeq, lastCommitSeq, revisionFilter, Long.MAX_VALUE);
    }

    public ReaderRecordIterator(
            CommitBoundedReader reader,
            Iterator<KeyTimelineEntry> timelines,
            long firstCommitSeq,
            long lastCommitSeq,
            long limit
    ) {
        this(reader, timelines, firstCommitSeq, lastCommitSeq, (_) -> true, limit);
    }


    public ReaderRecordIterator(
            CommitBoundedReader reader,
            Iterator<KeyTimelineEntry> timelines,
            long firstCommitSeq,
            long lastCommitSeq
    ) {
        this(reader, timelines, firstCommitSeq, lastCommitSeq, (_) -> true, Long.MAX_VALUE);
    }

    private void updateNext() {
        while (revision == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next.key();
            revision = next
                    .timeline()
                    .pin(firstCommitSeq, lastCommitSeq)
                    .map(KeyTimelineView::floor)
                    .filter(filter)
                    .orElse(null);
        }
    }

    @Override
    public boolean hasNext() {
        if (remaining <= 0) {
            return false;
        }
        updateNext();
        return revision != null;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        var next = reader.get(key, revision);
        revision = null;
        key = null;
        remaining--;

        return next;
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }
}
