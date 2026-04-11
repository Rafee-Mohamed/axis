package io.disys.axis.mvcc.store;

public record VersionedStoreConfig(
        int maxRevisionRecordBuffer,
        int revisionRecordBufferSyncTimeout,
        int deleteBatchSize,
        String versionDB,
        String metaDB,
        String persistedCommitSeq,
        String firstCommitSeq,
        String compactedRevision
) {
}
