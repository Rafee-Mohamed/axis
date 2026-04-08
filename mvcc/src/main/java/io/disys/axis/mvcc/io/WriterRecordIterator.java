package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.ModifiedAtSeqBound;
import io.disys.axis.mvcc.store.RecordIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class WriterRecordIterator implements RecordIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final WriteSession session;
    private final long lastCommitSeq;
    private byte[] key;
    private Revision revision;
    private final Predicate<Revision> filter;
    private long remaining;

    public WriterRecordIterator(WriteSession session, Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, Predicate<Revision> revisionFilter, long limit) {
        this.timelines = timelines;
        this.session = session;
        this.key = null;
        this.revision = null;
        this.lastCommitSeq = lastCommitSeq;
        this.filter = revisionFilter;
        this.remaining = limit;
    }

    public WriterRecordIterator(WriteSession session, Iterator<KeyTimelineEntry> timelines, long lastCommitSeq) {
       this(session, timelines, lastCommitSeq, (_) -> true, Long.MAX_VALUE);
    }

    public WriterRecordIterator(WriteSession session, Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, Predicate<Revision> revisionFilter) {
        this(session, timelines, lastCommitSeq, revisionFilter, Long.MAX_VALUE);
    }

    public WriterRecordIterator(WriteSession session, Iterator<KeyTimelineEntry> timelines, long lastCommitSeq, long limit) {
        this(session, timelines, lastCommitSeq, (_) -> true, limit);
    }

    private void updateNext() {
        while (revision == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next.key();
            revision = next.timeline()
                    .floor(lastCommitSeq)
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

        var next = session.get(key, revision);;
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

