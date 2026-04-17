package io.disys.axis.lease.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.backend.WriteHandle;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.*;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.random.RandomGenerator;

public class LeaseStore {
    private final Clock clock;
    private final Map<Long, Lease> leases;
    private final Map<LeaseItem, Long> leaseItems;
    private Optional<Scheduler> scheduler;
    private final LeaseStoreConfig config;
    private final LeaseDataEncoder encoder;
    private final LeaseDataDecoder decoder;
    private final RandomGenerator rand;
    private final Database db;


    LeaseStore(
            Clock clock,
            Map<Long, Lease> leases,
            Map<LeaseItem, Long> leaseItems,
            Optional<Scheduler> scheduler,
            LeaseStoreConfig config,
            LeaseDataEncoder encoder,
            LeaseDataDecoder decoder,
            Database db,
            RandomGenerator rand
    ) {
        this.clock = clock;
        this.leases = leases;
        this.scheduler = scheduler;
        this.leaseItems = leaseItems;
        this.config = config;
        this.encoder = encoder;
        this.decoder = decoder;
        this.db = db;
        this.rand = rand;
    }

    public static LeaseStoreState restoreState(Backend backend, LeaseStoreConfig config, Clock clock, RandomGenerator rand) {
        var db = Database.of(config.leaseDb());
        var encoder = new LeaseDataEncoder();
        var decoder = new LeaseDataDecoder();

        var bh = new LeaseBackendHandle(db, encoder, decoder);
        var leases = new HashMap<Long, Lease>();

        try (var txn = backend.beginRead()) {
            bh.consumeLeases(txn, (id, record) -> leases.put(id, Lease.restore(id, record, clock)));
        }

        return new LeaseStoreState(clock, leases, bh, config, rand);
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
                Database.of(state.config.leaseDb()),
                state.rand
        );
    }


    public GrantResult grant(WriteHandle wh, long ttl) {
        // should generate randomly
        var id = rand.nextLong(1, Long.MAX_VALUE);

        while (leases.containsKey(id)) {
            id = rand.nextLong(1, Long.MAX_VALUE);
        }

        return grant(wh, id, ttl);
    }

    public GrantResult grant(WriteHandle wh, long id, long ttl) {
        if (leases.containsKey(id)) {
            return new GrantResult.LeaseAlreadyExists(id);
        }
        var lease = Lease.start(id, ttl, clock);
        wh.put(db, encoder.encodeKey(id), encoder.encodeRecord(lease.ttl(), lease.remainingTtl()));
        return new GrantResult.LeaseGranted(id);
    }

    public RevokeResult revoke(WriteHandle wh, long id) {
        if (!leases.containsKey(id)) {
            return new RevokeResult.LeaseNotFound(id);
        }

        leases.compute(id, (_, lease) -> {
            lease.items().forEach(leaseItems::remove);
            wh.delete(db, encoder.encodeKey(id));
            return null;
        });

        return new RevokeResult.LeaseRevoked(id);
    }

    public RenewResult tryRenew(long id) {
        var lease = leases.get(id);
        if (lease == null) {
            // lease not found
            return new RenewResult.LeaseNotFound(id);
        }

        if (lease.expired()) {
            // expired wait - revoke in progress or still in queue but time has passed
            // so do we renew even after expired but expired not yet taken
            // of deadlines
            return new RenewResult.LeaseRevokeInProgress(id);
        }

        if (!lease.hasFullTtl()) {
            // return the checkpoint to be replicated
            // time has elapsed beyond at least one checkpoint
            return new RenewResult.LeaseCheckpoint(lease.id(), lease.ttl());
        }

        // can renew in memory
        lease.renew();
        return new RenewResult.LeaseRenewed(id, lease.remainingTtl());
    }

    public void checkpoint(long id, long remainingTtl) {
        leases.computeIfPresent(id, (_, lease) -> {
            lease.checkpoint(remainingTtl);
            return lease;
        });
    }

    public KeyAttachResult attach(long id, byte[] key) {
        var l = leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.put(lease.attach(key), id);
            return lease;
        });

        if (l == null) {
            return new KeyAttachResult.LeaseNotFound(id);
        }

        return new KeyAttachResult.Attached(id);
    }

    public KeyDetachResult detach(long id, byte[] key) {
        var l = leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.remove(lease.attach(key));
            return lease;
        });

        if (l == null) {
            return new KeyDetachResult.LeaseNotFound(id);
        }

        return new KeyDetachResult.Detached(id);
    }

    public void trackSchedules() {
        scheduler = Optional.of(scheduler.orElseGet(() ->
                Scheduler.fromLeases(leases.values(), clock, config.checkpointInterval())));
    }

    public void untrackSchedules() {
        scheduler = Optional.empty();
    }

    private Optional<Duration> nextSchedule() {
        return scheduler
                .flatMap(Scheduler::nextSchedule)
                .map(next -> {
                    var wakeAfter = Duration.between(clock.instant(), next);
                    if (wakeAfter.isNegative() || wakeAfter.isZero()) {
                        return Duration.ZERO;
                    }
                    return wakeAfter.compareTo(config.minWaitTime()) > 0 ? wakeAfter : config.minWaitTime();
                });
    }

    private List<Long> expired() {
        return scheduler.map(s -> s.expired(leases)).orElseGet(List::of);
    }

    private List<Checkpoint> checkpoints() {
        return scheduler.map(s -> s.checkpoints(leases)).orElseGet(List::of);
    }

}
