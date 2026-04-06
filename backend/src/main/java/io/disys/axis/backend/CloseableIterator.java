package io.disys.axis.backend;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterable<T>, Iterator<T>, AutoCloseable {}
