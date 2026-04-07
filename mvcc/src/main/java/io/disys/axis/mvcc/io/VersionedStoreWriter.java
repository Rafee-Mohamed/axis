package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.store.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.CloseableIterator;

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
    public void put(byte[] key, byte[] val)  {
        session.put(key, val, ordinal++);
    }

    @Override
    public boolean delete(byte[] key) {
        return session.delete(key, ordinal++);
    }

    @Override
    public ReadResult get(byte[] key) {
        return session.get(key);
    }

    @Override
    public ReadResult getAt(byte[] key, long commitSeq) {
        return session.getAt(key, commitSeq);
    }

    @Override
    public CloseableIterator<io.disys.axis.mvcc.model.Record> range(byte[] key, byte[] val) {
        return session.range(key, val);
    }

    @Override
    public CloseableIterator<io.disys.axis.mvcc.model.Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq) {
        return session.rangeAt(startKey, endKey, commitSeq);
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
