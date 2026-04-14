package io.disys.axis.lease.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.LeaseItem;
import io.disys.axis.mvcc.store.VersionedStore;

import java.time.Clock;
import java.time.Duration;
import java.util.*;

public class LeaseStore {
    private final Clock clock;
    private final Map<Long, Lease> leases;
    private final Map<LeaseItem, Long> leaseItems;
    private Optional<Scheduler> scheduler;
    private final VersionedStore store;
    private final LeaseStoreConfig config;



    LeaseStore(
            Clock clock,
            Map<Long, Lease> leases,
            Map<LeaseItem, Long> leaseItems,
            Optional<Scheduler> scheduler,
            VersionedStore store,
            LeaseStoreConfig config
    ) {
        this.clock = clock;
        this.leases = leases;
        this.scheduler = scheduler;
        this.leaseItems = leaseItems;
        this.store = store;
        this.config = config;
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

    static LeaseStore from(LeaseStoreState state, VersionedStore store) {
        var leaseItems = new HashMap<LeaseItem, Long>();
        var scheduler = new Scheduler(state.clock, state.config.checkpointInterval());

        for (var lease: state.leases.values()) {
            scheduler.schedule(lease);
            for (var item: lease.items()) {
                leaseItems.put(item, lease.id());
            }
        }

        return new LeaseStore(
                state.clock,
                state.leases,
                leaseItems,
                Optional.of(scheduler),
                store,
                state.config
        );
    }


    public void grant() {

    }

    public void revoke() {

    }

    public void renew() {

    }

    public void attach() {

    }

    public void detach() {

    }


    private Duration nextWake() {
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

}
