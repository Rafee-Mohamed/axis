package io.disys.axis.lease.store;

import java.time.Duration;

public record LeaseStoreConfig(
        long minLeaseTtl,
        int revokeRate,
        int checkpointBatchSize,
        int checkpointBatchRate,
        Duration checkpointInterval,
        Duration minWaitTime,
        Duration extendOnScheduleTrack,
        String leaseDb
) {
}
