package io.disys.axis.lease.store;

import java.time.Instant;

public record LeaseInstant(long id, Instant when, int renewalsAtSchedule) implements Comparable<LeaseInstant> {
    @Override
    public int compareTo(LeaseInstant o) {
        int c = this.when.compareTo(o.when);
        if (c != 0) return c;
        return Long.compare(this.id, o.id);
    }
}
