package io.disys.axis.storage.mvcc;

public sealed interface ReadResult {
    record Present(Record record) implements ReadResult {}
    record Absent() implements ReadResult {}
    record Compacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
    record Future(long lastVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
}
