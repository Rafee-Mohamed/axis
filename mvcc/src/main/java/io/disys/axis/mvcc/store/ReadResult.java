package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Record;

public sealed interface ReadResult {
    record Present(Record record) implements ReadResult {}
    record Absent() implements ReadResult {}
    record Compacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
    record Future(long lastVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
}
