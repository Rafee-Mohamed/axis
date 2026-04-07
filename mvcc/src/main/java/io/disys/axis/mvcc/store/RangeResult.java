package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

public sealed interface RangeResult {
    record Range(RecordIterator records) implements RangeResult {}
    record Compacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements RangeResult {}
    record Future(long lastVisibleCommitSeq, long requestedCommitSeq) implements RangeResult {}
}
