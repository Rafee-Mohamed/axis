package io.disys.axis.mvcc.store;

public sealed interface KeyRangeResult {
    record Range(KeyIterator keys) implements KeyRangeResult {}
    record Compacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements KeyRangeResult {}
    record Future(long lastVisibleCommitSeq, long requestedCommitSeq) implements KeyRangeResult {}
}
