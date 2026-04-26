package io.disys.axis.mvcc.model;

/**
 * The mutable visibility window of the store: the range of commit sequences
 * that readers and the current writer may observe.
 *
 * <pre>
 *   start               end    next()
 *     |                  |       |
 *  ---+---------...------+-------+------>  commitSeq
 * </pre>
 */
public class CommitSeqBound {

    private long start;

    /**
     * Declared {@code volatile} so that the write in {@link #advance()} establishes
     * happens-before with any subsequent read of this field during reader creation,
     * making the new committed bound visible across threads without synchronization.
     */
    private volatile long end;

    public CommitSeqBound(long start, long end) {
        this.start = start;
        this.end = end;
    }

    /**
     * @return the first {@code commitSeq} still visible; mutations at sequences
     *         strictly below this value have been physically removed by compaction
     */
    public long start() {
        return start;
    }

    /**
     * @return the last {@code commitSeq} that has been committed and is visible
     *         to new readers; volatile read
     */
    public long end() {
        return end;
    }

    /**
     * @return {@code end + 1}; the {@code commitSeq} the current commit will use
     *         for its mutations
     */
    public long next() {
        return end + 1;
    }

    /**
     * Updates {@code start} to {@code commitSeq} after compaction is initiated.
     *
     * @param commitSeq  the new first visible commit sequence
     */
    public void compact(long commitSeq) {
        start = commitSeq;
    }

    /**
     * Advances {@code end} by one after a commit completes. The volatile write
     * makes the new bound visible to readers on other threads.
     */
    public void advance() {
        ++end;
    }
}
