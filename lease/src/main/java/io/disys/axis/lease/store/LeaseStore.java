package io.disys.axis.lease.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.backend.WriteHandle;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.Checkpoint;
import io.disys.axis.lease.model.LeaseItem;
import io.disys.axis.mvcc.store.VersionedStore;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.*;

public class LeaseStore {
    private final Clock clock;
    private final Map<Long, Lease> leases;
    private final Map<LeaseItem, Long> leaseItems;
    private Optional<Scheduler> scheduler;
    private final LeaseStoreConfig config;
    private final LeaseDataEncoder encoder;
    private final LeaseDataDecoder decoder;

    private final Database db;


    LeaseStore(
            Clock clock,
            Map<Long, Lease> leases,
            Map<LeaseItem, Long> leaseItems,
            Optional<Scheduler> scheduler,
            LeaseStoreConfig config,
            LeaseDataEncoder encoder,
            LeaseDataDecoder decoder,
            Database db
    ) {
        this.clock = clock;
        this.leases = leases;
        this.scheduler = scheduler;
        this.leaseItems = leaseItems;
        this.config = config;
        this.encoder = encoder;
        this.decoder = decoder;
        this.db = db;
    }

    public static LeaseStoreState restoreState(Backend backend, LeaseStoreConfig config, Clock clock) {
        var db = Database.of(config.leaseDb());
        var encoder = new LeaseDataEncoder();
        var decoder = new LeaseDataDecoder();

        var bh = new LeaseBackendHandle(db, encoder, decoder);
        var leases = new HashMap<Long, Lease>();

        try (var txn = backend.beginRead()) {
            bh.consumeLeases(txn, (id, record) -> leases.put(id, Lease.restore(id, record, clock)));
        }

        return new LeaseStoreState(clock, leases, bh, config);
    }

    static LeaseStore from(LeaseStoreState state) {
        var leaseItems = new HashMap<LeaseItem, Long>();
        var scheduler = new Scheduler(state.clock, state.config.checkpointInterval());

        for (var lease: state.leases.values()) {
            scheduler.schedule(lease);
            for (var item: lease.items()) {
                leaseItems.put(item, lease.id());
            }
        }

        var encoder = new LeaseDataEncoder();
        var decoder = new LeaseDataDecoder();

        return new LeaseStore(
                state.clock,
                state.leases,
                leaseItems,
                Optional.of(scheduler),
                state.config,
                encoder,
                decoder,
                Database.of(state.config.leaseDb())
        );
    }


    public long grant(WriteHandle wh, long ttl) {
        // should generate randomly
        var id = 0;
        return grant(wh, id, ttl);
    }

    public long grant(WriteHandle wh, long id, long ttl) {
        var lease = Lease.start(id, ttl, clock);
        wh.put(db, encoder.encodeKey(id), encoder.encodeRecord(lease.ttl(), lease.remainingTtl()));
        return id;
    }

    public void revoke(WriteHandle wh, long id) {
        leases.computeIfPresent(id, (_, lease) -> {
            leases.remove(id).items().forEach(leaseItems::remove);
            wh.delete(db, encoder.encodeKey(id));
            return lease;
        });
    }

    public void renew(long id) {
        var lease = leases.get(id);
        if (lease == null) {
            // lease not found
            return;
        }
    }

    public void attach(long id, byte[] key) {
        leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.put(lease.attach(key), id);
            return lease;
        });
    }

    public void detach(long id, byte[] key) {
        leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.remove(lease.attach(key));
            return lease;
        });
    }

    private Duration nextSchedule() {
        return scheduler
                .flatMap(Scheduler::nextSchedule)
                .map(next -> {
                    var wakeAfter = Duration.between(clock.instant(), next);
                    if (wakeAfter.isNegative() || wakeAfter.isZero()) {
                        return Duration.ZERO;
                    }
                    return wakeAfter.compareTo(config.minWaitTime()) > 0 ? wakeAfter : config.minWaitTime();
                })
                .orElseGet(config::minWaitTime);
    }

    private List<Long> expired() {
        return scheduler.map(s -> s.expired(leases)).orElseGet(List::of);
    }

    private List<Checkpoint> checkpoints() {
        return scheduler.map(s -> s.checkpoints(leases)).orElseGet(List::of);
    }

}
