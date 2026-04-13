package io.disys.axis.lease.store;

import io.disys.axis.lease.model.LeaseItem;
import io.disys.axis.lease.model.LeaseRecord;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class Lease {
    private final long id;
    private final Instant expiry;
    private final long ttl;
    private long remainingTtl;
    private final Set<LeaseItem> items;


    public Lease(long id, long ttl, long remainingTtl, Instant expiry, Set<LeaseItem> items) {
        this.id = id;
        this.ttl = ttl;
        this.remainingTtl = remainingTtl;
        this.expiry = expiry;
        this.items = items;
    }

    public static Lease start(long id, Instant expiry, long ttl) {
        return new Lease(id, ttl, 0, expiry, new HashSet<>());
    }

    public static Lease restore(long id, LeaseRecord record, Clock clock) {
        return new Lease(id, record.ttl(), record.remainingTtl(), clock.instant(), new HashSet<>());
    }

    long id() {
        return id;
    }

    long ttl() {
        return ttl;
    }

    long remainingTtl() {
        return remainingTtl;
    }


    public void addItem(byte[] key) {
        items.add(new LeaseItem(key));
    }
}
