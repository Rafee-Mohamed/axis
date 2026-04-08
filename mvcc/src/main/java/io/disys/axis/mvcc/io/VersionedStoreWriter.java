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
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound) {
        return session.range(from, to, bound);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, long limit) {
        return session.range(from, to, limit);
    }

    @Override
    public RecordIterator range(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit) {
        return session.range(from, to, bound, limit);
    }

    @Override
    public RangeResult rangeAt(byte[] start, byte[] end, long commitSeq) {
        return session.rangeAt(start, end, commitSeq);
    }

    @Override
    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound) {
        return session.rangeAt(from, to, commitSeq, bound);
    }

    @Override
    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, long limit) {
        return  session.rangeAt(from, to, commitSeq, limit);
    }

    @Override
    public RangeResult rangeAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit) {
        return session.rangeAt(from, to, commitSeq, bound, limit);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to) {
        return session.keys(from, to);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound) {
        return session.keys(from, to, bound);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, long limit) {
        return session.keys(from, to, limit);
    }

    @Override
    public KeyIterator keys(byte[] from, byte[] to, ModifiedAtSeqBound bound, long limit) {
        return session.keys(from, to, bound, limit);
    }

    @Override
    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq) {
        return session.keysAt(from, to, commitSeq);
    }

    @Override
    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound) {
        return session.keysAt(from, to, commitSeq, bound);
    }

    @Override
    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, long limit) {
        return session.keysAt(from, to, commitSeq, limit);
    }

    @Override
    public KeyRangeResult keysAt(byte[] from, byte[] to, long commitSeq, ModifiedAtSeqBound bound, long limit) {
        return session.keysAt(from, to, commitSeq, bound, limit);
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
