package io.disys.axis.mvcc.model;

/**
 * Result type for time-travel queries; signals whether the requested commitSeq
 * was within the visible window or fell outside it.
 */
public sealed interface SnapshotResult<T> {
    /** The requested commitSeq was in range; carries the query result. */
    record Ok<T>(T value) implements SnapshotResult<T> {}

    /**
     * The requested commitSeq has been compacted away.
     *
     * @param firstVisibleCommitSeq the earliest commitSeq still queryable
     * @param requestedCommitSeq    the commitSeq that was requested
     */
    record Compacted<T>(long firstVisibleCommitSeq, long requestedCommitSeq) implements SnapshotResult<T> {}

    /**
     * The requested commitSeq is beyond the current state.
     *
     * @param lastVisibleCommitSeq the latest commitSeq currently available
     * @param requestedCommitSeq   the commitSeq that was requested
     */
    record Future<T>(long lastVisibleCommitSeq, long requestedCommitSeq) implements SnapshotResult<T> {}
}
