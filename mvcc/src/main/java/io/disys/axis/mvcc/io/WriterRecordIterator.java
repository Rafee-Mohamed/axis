package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.store.RecordIterator;
import io.disys.axis.mvcc.timeline.KeyTimelineEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class WriterRecordIterator implements RecordIterator {
    private final Iterator<KeyTimelineEntry> timelines;
    private final WriteSession session;
    private final long lastCommitSeq;
    private byte[] key;
    private Revision revision;

    public WriterRecordIterator(WriteSession session, Iterator<KeyTimelineEntry> timelines, long lastCommitSeq) {
        this.timelines = timelines;
        this.session = session;
        this.key = null;
        this.revision = null;
        this.lastCommitSeq = lastCommitSeq;
    }

    private void updateNext() {
        while (revision == null && timelines.hasNext()) {
            var next = timelines.next();
            key = next.key();
            revision = next.timeline()
                    .floor(lastCommitSeq)
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

        var next = session.get(key, revision);;
        revision = null;
        key = null;

        return next;
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }
}

