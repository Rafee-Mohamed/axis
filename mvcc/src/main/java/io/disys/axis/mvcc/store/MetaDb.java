package io.disys.axis.mvcc.store;

import io.disys.axis.backend.Database;

public record MetaDb(
        String name,
        byte[] persistedCommitSeqKey,
        byte[] firstCommitSeqKey,
        byte[] compactedRevisionKey,
        byte[] completedCompactionCommitSeqKey
) implements Database {
}
