package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.RecordIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;
import io.disys.axis.mvcc.timeline.KeyTimelineView;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ReaderRecordIterator implements RecordIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final VersionedStoreReader reader;
    private final long firstCommitSeq;
    private final long lastCommitSeq;
    private byte[] key;
    private Revision revision;

    public ReaderRecordIterator(VersionedStoreReader reader, Iterator<KeyTimelineEntry> timelines, long firstCommitSeq, long lastCommitSeq) {
        this.timelines = timelines;
        this.reader = reader;
        this.key = null;
        this.revision = null;
        this.firstCommitSeq = firstCommitSeq;
        this.lastCommitSeq = lastCommitSeq;
    }

    private void updateNext() {
        while (revision == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next.key();
            revision = next
                    .timeline()
                    .pin(firstCommitSeq, lastCommitSeq)
                    .map(KeyTimelineView::floor)
                    .orElse(null);
        }
    }

    @Override
    public boolean hasNext() {
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

        return next;
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }
}
