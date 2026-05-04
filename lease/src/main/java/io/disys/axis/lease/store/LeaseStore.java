package io.disys.axis.lease.store;

import io.disys.axis.backend.Backend;
import io.disys.axis.backend.Database;
import io.disys.axis.backend.WriteHandle;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.*;
import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.model.Record;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.function.BiConsumer;

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

    public static LeaseStore restore(Backend backend, LeaseStoreConfig config, Clock clock) {
        var db = Database.of(config.leaseDb());
        var encoder = new LeaseDataEncoder();
        var decoder = new LeaseDataDecoder();

        var bh = new LeaseBackendHandle(db, encoder, decoder);
        var leases = new HashMap<Long, Lease>();

        try (var txn = backend.beginRead()) {
            bh.consumeLeases(txn, (id, record) -> leases.put(id, Lease.restore(id, record, clock)));
        }

        var leaseItems = new HashMap<LeaseItem, Long>();

        return new LeaseStore(
                clock,
                leases,
                leaseItems,
                Optional.empty(),
                config,
                encoder,
                decoder,
                Database.of(config.leaseDb())
        );
    }


    public GrantResult grant(WriteHandle wh, long id, long ttl) {
        if (leases.containsKey(id)) {
            return new GrantResult.LeaseAlreadyExists(id);
        }
        var lease = Lease.start(id, ttl, clock);
        leases.put(id, lease);
        scheduler.ifPresent(s -> s.schedule(lease));
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

        scheduler.ifPresent(s -> s.revoke(id));

        return new RevokeResult.LeaseRevoked(id);
    }

    // renew only called when schedules are tracked - that is only by leader
    public RenewResult tryRenew(long id) {
        var lease = leases.get(id);
        if (lease == null) {
            // lease not found
            return new RenewResult.LeaseNotFound(id);
        }

        if (scheduler.map(s -> s.expired(lease.id())).orElse(false)) {
            // the scheduler already surfaced this lease for expired leases
            // so the revoke in progress therefore upstream system can wait for revoke
            // to complete and return lease not found or can renew which will eventually fail
            return new RenewResult.LeaseRevokeInProgress(id);
        }

        // if reached here means that scheduler haven't yet queried with expired
        // but the deadline might have been reached for this lease, but it is not
        // surfaced by the upstream system by calling expired, therefore it is safe
        // to renew if it has full ttl in that case the lease will live again
        // as the checkpoint is invalidated by new renewal of lease
        // otherwise a checkpoint will be surfaced for renewal followed by
        // surfacing expired, when renew checkpoint runs before or after,
        // the lease will be removed by revoke surfaced by scheduler
        // if revoke is from scheduler, and renew is in hands from client
        // then the upstream system can check after renew and expired if tryRenew
        // returned checkpoint and expired returned the same lease, then
        // it has to be handled carefully, since it will fail and renew
        // is not correct with respect to client as renew succeeded but the
        // scheduled expiry before or after will remove the lease, therefore
        // if lease checkpoint is surfaced then the schedule for those lease
        // don't need to appear, as checkpoint for that lease is already surfaced
        // if scheduler is called first, then revoke in progress will be returned
        // and after lease checkpoint, so ordering - tryRenew -> scheduler

        if (!lease.hasFullTtl()) {
            // return the checkpoint to be replicated
            // time has elapsed beyond at least one checkpoint
            // renewInProgress invalidates the lease checkpoint/deadline
            lease.renewInProgress();
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

    public BiConsumer<Revision, Record> keysRecoveryConsumer() {
        return (_, record) -> {
            var id = record.tombstone() ? 0L : decoder.decodeLeaseId(record.val());
            if (id != 0) {
                attachToLease(id, record.key());
            } else {
                // if no lease is attached to the key now - might be because of
                // tombstone or else no lease (0) attached, then check
                // if the key is already present for any lease, if so
                // detach the lease from the key
                var currentId = leaseItems.get(LeaseItem.of(record.key()));
                if (currentId != null) {
                    detachFromLease(currentId, record.key());
                }
            }
        };
    }

    public KeyAttachResult attach(long id, byte[] key) {
        var lease = attachToLease(id, key);
        if (lease == null) {
            return new KeyAttachResult.LeaseNotFound(id);
        }

        return new KeyAttachResult.Attached(id);
    }

    private Lease attachToLease(long id, byte[] key) {
        var item = LeaseItem.of(key);
        var currentId = leaseItems.get(item);

        if (currentId != null && currentId != id) {
            detachFromLease(currentId, key);
        }

        return leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.put(lease.attach(key), id);
            return lease;
        });
    }

    public List<byte[]> leasedKeys(long id) {
        var lease = leases.get(id);
        if (lease == null) {
            return List.of();
        }
        return lease.items().stream().map(LeaseItem::key).toList();
    }

    public KeyDetachResult detach(long id, byte[] key) {
        var lease = detachFromLease(id, key);

        if (lease == null) {
            return new KeyDetachResult.LeaseNotFound(id);
        }

        return new KeyDetachResult.Detached(id);
    }

    public void detachIfLeased(byte[] key) {
        var id = leaseItems.get(LeaseItem.of(key));
        if (id != null) {
            detachFromLease(id, key);
        }
    }

    private Lease detachFromLease(long id, byte[] key) {
        return leases.computeIfPresent(id, (_, lease) -> {
            leaseItems.remove(lease.detach(key));
            return lease;
        });
    }


    public void trackSchedules() {
        scheduler = Optional.of(scheduler.orElseGet(() ->
                Scheduler.fromLeases(leases.values(), clock, config)));
    }

    public void untrackSchedules() {
        scheduler = Optional.empty();
    }

    public Optional<Duration> nextSchedule() {
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

    public List<Long> expired() {
        return scheduler.map(s -> s.expired(leases)).orElseGet(List::of);
    }

    public List<List<Checkpoint>> checkpoints() {
        return scheduler.map(s -> s.checkpoints(leases)).orElseGet(List::of);
    }

    public Optional<LeaseInfo> leaseInfo(long id) {
        var lease = leases.get(id);
        if (lease == null) return Optional.empty();
        var keys = lease.items().stream().map(LeaseItem::key).toList();
        return Optional.of(new LeaseInfo(lease.id(), lease.ttl(), lease.remainingTtl(), keys));
    }

    public List<LeaseInfo> leaseList() {
        return leases.values().stream()
                .map(l -> new LeaseInfo(l.id(), l.ttl(), l.remainingTtl(), List.of()))
                .toList();
    }

}
