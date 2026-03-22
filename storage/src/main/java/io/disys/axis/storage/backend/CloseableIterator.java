package io.disys.axis.storage.backend;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {}
