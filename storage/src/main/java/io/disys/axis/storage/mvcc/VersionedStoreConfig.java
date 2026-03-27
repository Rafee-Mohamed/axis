package io.disys.axis.storage.mvcc;

public record VersionedStoreConfig(
        int maxRevisionRecordBuffer,
        int revisionRecordBufferSyncTimeout,
        String versionDB,
        String metaDB,
        String persistedCommitSeq,
        String firstCommitSeq
) {
}
