package io.disys.axis.backend.lmdb;

import io.disys.axis.backend.CloseableIterator;
import io.disys.axis.backend.KeyVal;
import org.lmdbjava.CursorIterable;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class LmdbClosableIterator implements CloseableIterator<KeyVal> {
    private final CursorIterable<ByteBuffer> iterable;
    private final Iterator<CursorIterable.KeyVal<ByteBuffer>> inner;

    LmdbClosableIterator(CursorIterable<ByteBuffer> iterable) {
        this.iterable = iterable;
        this.inner = iterable.iterator();
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public KeyVal next() {
        var kv = inner.next();
        return new KeyVal(LmdbUtil.toBytes(kv.key()), LmdbUtil.toBytes(kv.val()));
    }

    @Override
    public Iterator<KeyVal> iterator() {
        return this;
    }

    @Override
    public void close() {
        iterable.close();
    }
}
