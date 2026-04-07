package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public interface RecordIterator extends Iterator<Record>, Iterable<Record> {
}
