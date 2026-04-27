package io.disys.axis.mvcc.store;

import io.disys.axis.backend.Database;

/**
 * Backend database holding store metadata; each entry is an 8-byte {@code long} stored under its key.
 *
 * @param name                            backend database name
 * @param persistedCommitSeqKey           key for the last commit seq flushed to the backend
 * @param firstCommitSeqKey               key for the compaction boundary (first visible commit seq)
 * @param compactedRevisionKey            key for the compacted revision marker
 * @param completedCompactionCommitSeqKey key for the commit seq at which the last compaction batch
 *                                        ran to completion; read on restore to determine whether
 *                                        a batch must be resumed
 */
public record MetaDb(
        String name,
        byte[] persistedCommitSeqKey,
        byte[] firstCommitSeqKey,
        byte[] compactedRevisionKey,
        byte[] completedCompactionCommitSeqKey
) implements Database {
}
