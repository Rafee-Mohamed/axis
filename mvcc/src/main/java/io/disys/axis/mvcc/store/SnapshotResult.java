package io.disys.axis.mvcc.store;

public sealed interface SnapshotResult<T> {
    record Ok<T>(T value) implements SnapshotResult<T> {}
    record Compacted<T>(long firstVisibleCommitSeq, long requestedCommitSeq) implements SnapshotResult<T> {}
    record Future<T>(long lastVisibleCommitSeq, long requestedCommitSeq) implements SnapshotResult<T> {}
}
