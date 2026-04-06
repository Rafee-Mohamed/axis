package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.timeline.*;

public record VersionedStoreConfig(
        int maxRevisionRecordBuffer,
        int revisionRecordBufferSyncTimeout,
        String versionDB,
        String metaDB,
        String persistedCommitSeq,
        String firstCommitSeq
) {
}
