package io.disys.axis.lease.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.LeaseItem;
import io.disys.axis.mvcc.store.VersionedStore;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LeaseStore {
    private final Clock clock;
    private final Map<Long, Lease> leaseMap;
    private final PriorityQueue<LeaseInstant> leaseDeadlines;
    private final PriorityQueue<LeaseInstant> leaseCheckpoints;
    private final Map<LeaseItem, Long> leaseItemMap;
    private final VersionedStore store;
    private final LeaseStoreConfig config;

    private final BlockingQueue<Event> events;

    sealed interface Event {
        record Grant() implements Event {}
        record Revoke(long id) implements Event {}
        record Renew(long id) implements Event {}
    }
    LeaseStore(
            Clock clock,
            Map<Long, Lease> leaseMap,
            PriorityQueue<LeaseInstant> leaseDeadlines,
            PriorityQueue<LeaseInstant> leaseCheckpoints,
            Map<LeaseItem, Long> leaseItemMap,
            VersionedStore store,
            LeaseStoreConfig config,
            BlockingQueue<Event> events
    ) {
        this.clock = clock;
        this.leaseMap = leaseMap;
        this.leaseDeadlines = leaseDeadlines;
        this.leaseCheckpoints = leaseCheckpoints;
        this.leaseItemMap = leaseItemMap;
        this.store = store;
        this.config = config;
        this.events = events;
    }

    static LeaseStore fromState(LeaseStoreState state, VersionedStore store) {
        return new LeaseStore(
                state.clock,
                state.leaseMap,
                new PriorityQueue<>(),
                new PriorityQueue<>(),
                new HashMap<>(),
                store,
                state.config,
                new LinkedBlockingQueue<>()
        );
    }

    public static LeaseStoreState restoreState(Backend backend, LeaseStoreConfig config, Clock clock) {
        var db = Database.of(config.leaseDb());
        var encoder = new LeaseDataEncoder();
        var decoder = new LeaseDataDecoder();

        var bh = new LeaseBackendHandle(db, encoder, decoder);
        var leaseMap = new HashMap<Long, Lease>();

        try (var txn = backend.beginRead()) {
            bh.consumeLeases(txn, (id, record) -> leaseMap.put(id, Lease.restore(id, record, clock)));
        }

        return new LeaseStoreState(clock, leaseMap, bh, config);
    }


}
