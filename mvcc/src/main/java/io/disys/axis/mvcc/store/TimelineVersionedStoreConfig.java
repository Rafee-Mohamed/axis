package io.disys.axis.mvcc.store;

import java.time.Duration;

/**
 * Configuration for {@link TimelineVersionedStore}.
 *
 * @param maxRevisionRecordBuffer        maximum number of {@link io.disys.axis.mvcc.model.RevisionRecord}s
 *                                       the buffer may hold before the active write session is forced to commit
 * @param revisionRecordBufferSyncTimeout wall-clock timeout after which the active
 *                                       write session expires and is committed on the next writer vend
 * @param deleteBatchSize                number of revision records removed per compaction batch step
 * @param indexMaxKeys                   maximum keys per node in the B+ tree timeline index
 * @param versionDB                      backend database name that stores revision records
 * @param metaDB                         backend database name that stores meta entries
 *                                       (persisted commit seq, compaction boundary, etc.)
 * @param persistedCommitSeq             meta key name for the last commit seq flushed to the backend
 * @param firstCommitSeq                 meta key name for the compaction boundary (first visible commit seq)
 * @param compactedRevision              meta key name for the compacted revision marker
 * @param completedCompactionCommitSeq   meta key name tracking the commit seq at which the last
 *                                       compaction batch completed cleanly
 */
public record TimelineVersionedStoreConfig(
        int maxRevisionRecordBuffer,
        Duration revisionRecordBufferSyncTimeout,
        int deleteBatchSize,
        int indexMaxKeys,
        String versionDB,
        String metaDB,
        String persistedCommitSeq,
        String firstCommitSeq,
        String compactedRevision,
        String completedCompactionCommitSeq
) {
}
