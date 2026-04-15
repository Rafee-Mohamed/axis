package io.disys.axis.lease.store;

import io.disys.axis.lease.model.LeaseItem;
import io.disys.axis.lease.model.LeaseRecord;

import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class Lease {
    private final long id;
    private Instant expiry;
    private final long ttl;
    private long remainingTtl;
    private final Set<LeaseItem> items;
    private final Clock clock;
    private int renewals;


    Lease(long id, long ttl, long remainingTtl, Clock clock, Instant expiry, Set<LeaseItem> items) {
        this.id = id;
        this.ttl = ttl;
        this.remainingTtl = remainingTtl;
        this.expiry = expiry;
        this.items = items;
        this.clock = clock;
        this.renewals = 0;
    }

    // ttl == remainingTtl at start
    public static Lease start(long id,  long ttl, Clock clock) {
        return new Lease(id, ttl, ttl, clock, clock.instant().plusSeconds(ttl), new HashSet<>());
    }

    public static Lease restore(long id, LeaseRecord record, Clock clock) {
        return new Lease(
                id,
                record.ttl(),
                record.remainingTtl(),
                clock,
                clock.instant().plusSeconds(record.remainingTtl()),
                new HashSet<>()
        );
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

    Instant expiry() {
        return expiry;
    }

    Set<LeaseItem> items() {
        return items;
    }

    int renewals() {
        return renewals;
    }


    LeaseItem attach(byte[] key) {
        var item = LeaseItem.of(key);
        items.add(item);
        return item;
    }

    LeaseItem detach(byte[] key) {
        var item = LeaseItem.of(key);
        items.remove(item);
        return item;
    }

    void renew() {
        renewals++;
        remainingTtl = 0;
        expiry = clock.instant().plusSeconds(ttl);
    }

    void setRemainingTtl(long ttl) {
        remainingTtl = ttl;
    }
}
