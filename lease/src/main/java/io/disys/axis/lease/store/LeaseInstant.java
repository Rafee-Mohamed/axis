package io.disys.axis.lease.store;

import java.time.Instant;

public record LeaseInstant(long id, Instant instant) {
}
