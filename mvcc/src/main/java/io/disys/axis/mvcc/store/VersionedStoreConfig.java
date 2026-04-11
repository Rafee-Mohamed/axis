package io.disys.axis.mvcc.store;

public record VersionedStoreConfig(
        int maxRevisionRecordBuffer,
        int revisionRecordBufferSyncTimeout,
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
