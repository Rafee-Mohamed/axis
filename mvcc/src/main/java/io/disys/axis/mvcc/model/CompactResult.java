package io.disys.axis.mvcc.model;

public sealed interface CompactResult {
    record Ok() implements CompactResult {}
    record AlreadyCompacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements CompactResult {}
    record FutureRevision(long lastVisibleCommitSeq, long requestedCommitSeq) implements CompactResult {}
    record InProgress() implements CompactResult {}
}

