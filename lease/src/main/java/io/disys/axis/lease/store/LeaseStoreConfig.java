package io.disys.axis.lease.store;

import java.time.Duration;

public record LeaseStoreConfig(
        long minLeaseTtl,
        int leaseRevokeRate,
        // at least should be 1
        Duration checkpointInterval,
        Duration expiredLeaseRetryInterval,
        Duration minWaitTime,
        String leaseDb
) {
}
