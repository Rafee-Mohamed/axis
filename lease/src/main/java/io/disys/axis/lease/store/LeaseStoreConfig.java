package io.disys.axis.lease.store;

import java.time.Duration;

public record LeaseStoreConfig(
        long minLeaseTtl,
        int leaseRevokeRate,
        Duration checkpointInterval,
        Duration expiredLeaseRetryInterval,
        String leaseDb
) {
}
