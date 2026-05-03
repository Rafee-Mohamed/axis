package io.disys.axis.lease.model;

import java.util.List;

public record LeaseInfo(long id, long ttl, long remainingTtl, List<byte[]> keys) {
}
