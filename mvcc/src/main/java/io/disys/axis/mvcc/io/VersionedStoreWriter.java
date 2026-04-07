package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.model.Record;

import java.util.Optional;

public class VersionedStoreWriter implements Writer {

    private final WriteSession session;
    private int ordinal;

    public VersionedStoreWriter(WriteSession session) {
        this.session = session;
        this.ordinal = 0;
    }

    @Override
    public void put(byte[] key, byte[] val)  {
        session.put(key, val, ordinal++);
    }

    @Override
    public boolean delete(byte[] key) {
        return session.delete(key, ordinal++);
    }

    @Override
    public Optional<Record> get(byte[] key) {
        return session.get(key);
    }

    @Override
    public ReadResult getAt(byte[] key, long commitSeq) {
        return session.getAt(key, commitSeq);
    }

    @Override
    public RecordIterator range(byte[] start, byte[] end) {
        return session.range(start, end);
    }

    @Override
    public RangeResult rangeAt(byte[] start, byte[] end, long commitSeq) {
        return session.rangeAt(start, end, commitSeq);
    }

    @Override
    public void close() {
        if (ordinal == 0) {
            // nothing written so no need to advance
            return;
        }
        session.advance();
    }
}
