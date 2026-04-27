package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.*;

/**
 * A single-writer, multiple-reader multi-version concurrency control (MVCC) store.
 *
 * <h2>Concurrency</h2>
 * <p>{@link #reader()} is safe to call concurrently from multiple threads; each call
 * returns an independent {@link Reader} pinned to the committed state at the moment
 * of creation.</p>
 *
 * <p>{@link #writer()}, {@link #compact(long)}, and {@link #sync()} must be driven
 * by a single thread; concurrent invocations have undefined behaviour.</p>
 *
 * <h2>Durability</h2>
 * <p>Logical commits ({@link Writer#close()}) are immediately visible to new readers.
 * Durability — surviving crashes and restarts — is guaranteed only after
 * {@link #sync()} returns.</p>
 */
public interface VersionedStore {

    /** Returns a {@link Reader} pinned to the current committed state. Thread-safe. */
    Reader reader();

    /**
     * Returns the active {@link Writer}. All writes staged on the writer land at the same
     * {@code commitSeq}; {@link Writer#close()} commits them and makes them visible to readers.
     * Not thread-safe.
     */
    Writer writer();

    /**
     * Sets {@code commitSeq} as the compaction boundary. After this returns
     * {@link CompactResult.Ok}, reads at any {@code commitSeq} strictly below
     * this value return {@link SnapshotResult.Compacted}.
     * Not thread-safe.
     *
     * @param commitSeq the new compaction boundary; commits strictly below this value
     *                  will no longer be visible, commits at or above it remain queryable
     * @return {@link CompactResult.Ok} if accepted;
     *         {@link CompactResult.InProgress} if a previous compaction has not finished;
     *         {@link CompactResult.AlreadyCompacted} if the boundary already meets or
     *         exceeds {@code commitSeq};
     *         {@link CompactResult.FutureRevision} if {@code commitSeq} has not yet been committed
     */
    CompactResult compact(long commitSeq);

    /**
     * Guarantees that all logical commits made before this call are durable.
     * they survive crashes and are recoverable on restart.
     * Not thread-safe.
     */
    void sync();
}
