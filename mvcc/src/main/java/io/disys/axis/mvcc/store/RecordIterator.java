package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

import java.util.Iterator;

public interface RecordIterator extends Iterator<Record>, Iterable<Record> {
}
