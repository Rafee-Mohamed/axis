package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public class VersionedStoreWriter implements Writer {

    private final WriteSession session;
    private int ordinal;

    public VersionedStoreWriter(WriteSession session) {
        this.session = session;
        this.ordinal = 0;
    }

    @Override
    public void put(byte[] key, byte[] val) throws IOException {
        session.put(key, val, ordinal++);
    }

    @Override
    public boolean delete(byte[] key) throws IOException {
        return session.delete(key, ordinal++);
    }

    @Override
    public ReadResult get(byte[] key) throws IOException {
        return session.get(key);
    }

    @Override
    public ReadResult getAt(byte[] key, long commitSeq) throws IOException {
        return session.getAt(key, commitSeq);
    }

    @Override
    public CloseableIterator<Record> range(byte[] key, byte[] val) {
        return session.range(key, val);
    }

    @Override
    public CloseableIterator<Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq) {
        return session.rangeAt(startKey, endKey, commitSeq);
    }

    @Override
    public void close() throws IOException {
        if (ordinal == 0) {
            // nothing written so no need to advance
            return;
        }
        session.advance();
    }
}
