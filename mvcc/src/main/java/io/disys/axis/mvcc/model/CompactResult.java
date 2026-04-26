package io.disys.axis.mvcc.model;

/** Result of a compaction request. */
public sealed interface CompactResult {

    /** Compaction accepted; batch deletion scheduled. */
    record Ok() implements CompactResult {}

    /**
     * The requested {@code commitSeq} is at or below the current compaction
     * boundary; there is nothing to compact.
     *
     * @param firstVisibleCommitSeq  the current compaction boundary; the lowest
     *                               {@code commitSeq} still visible in the store
     * @param requestedCommitSeq     the {@code commitSeq} that was requested
     */
    record AlreadyCompacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements CompactResult {}

    /**
     * The requested {@code commitSeq} exceeds the last committed sequence;
     * the revision does not yet exist.
     *
     * @param lastVisibleCommitSeq  the highest {@code commitSeq} currently committed
     * @param requestedCommitSeq    the {@code commitSeq} that was requested
     */
    record FutureRevision(long lastVisibleCommitSeq, long requestedCommitSeq) implements CompactResult {}

    /** A previous compaction batch is still running; retry after it completes. */
    record InProgress() implements CompactResult {}
}
