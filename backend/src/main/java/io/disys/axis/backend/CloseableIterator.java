package io.disys.axis.backend;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterable<T>, Iterator<T>, AutoCloseable {
    // suppress exception thrown in AutoClosable
    // thrown exceptions are runtime exception - Backend Exception
    @Override
    void close();
}
