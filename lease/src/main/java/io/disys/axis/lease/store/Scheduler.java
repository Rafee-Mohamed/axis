package io.disys.axis.lease.store;

import io.disys.axis.lease.model.Checkpoint;

import java.time.Clock;
import java.time.Instant;
import java.util.*;

public class Scheduler {
    private final PriorityQueue<LeaseInstant> checkpoints;
    private final PriorityQueue<LeaseInstant> deadlines;
    private final Set<Long> revokeInProgress;
    private final LeaseStoreConfig config;
    private final Clock clock;


    public Scheduler(Clock clock, LeaseStoreConfig config) {
        this.checkpoints = new PriorityQueue<>();
        this.deadlines = new PriorityQueue<>();
        this.revokeInProgress = new HashSet<>();
        this.clock = clock;
        this.config = config;
    }


    public static Scheduler fromLeases(Collection<Lease> leases, Clock clock, LeaseStoreConfig config) {
        var scheduler = new Scheduler(clock, config);
        for (var lease: leases) {
            lease.extendExpiry(config.extendOnScheduleTrack());
            scheduler.schedule(lease);
        }
        return scheduler;
    }

    public void schedule(Lease lease) {
        deadlines.add(new LeaseInstant(lease.id(), lease.expiry(), lease.renewals()));

        if (lease.remainingTtl() <= config.checkpointInterval().toSeconds()) {
            return;
        }
        var nextCheckpoint = clock.instant().plus(config.checkpointInterval());
        checkpoints.add(new LeaseInstant(lease.id(), nextCheckpoint, lease.renewals()));
    }


    public Optional<Instant> nextSchedule() {
        if (deadlines.isEmpty() && checkpoints.isEmpty()) {
            return Optional.empty();
        }

        if (deadlines.isEmpty()) {
            return Optional.of(checkpoints.peek().when());
        }

        if (checkpoints.isEmpty()) {
            return Optional.of(deadlines.peek().when());
        }

        var next = deadlines.peek().when().isBefore(checkpoints.peek().when())
                ? deadlines.peek().when()
                : checkpoints.peek().when();

        return Optional.of(next);
    }

    public boolean expired(long id) {
        return revokeInProgress.contains(id);
    }

    public void revoke(long id) {
        revokeInProgress.remove(id);
    }

    public List<Long> expired(Map<Long, Lease> leases) {
        var expiredLeases = new ArrayList<Long>();
        var now = clock.instant();

        while (expiredLeases.size() < config.revokeRate() && !deadlines.isEmpty() && deadlines.peek().when().isBefore(now)) {
            var next = deadlines.poll();
            var lease = leases.get(next.id());

            if (lease == null) {
                continue;
            }

            // lease changed since scheduled
            if (next.renewalsAtSchedule() < lease.renewals()) {
                continue;
            }

            // epoch didn't change since schedule
            expiredLeases.add(lease.id());
            revokeInProgress.add(lease.id());
        }

        return expiredLeases;
    }

    public List<List<Checkpoint>> checkpoints(Map<Long, Lease> leases) {
        var checkpointsBatch = new ArrayList<List<Checkpoint>>();
        var batch = new ArrayList<Checkpoint>();
        var now = clock.instant();

        while (checkpointsBatch.size() < config.checkpointBatchRate() && !checkpoints.isEmpty() && checkpoints.peek().when().isBefore(now)) {
            var next = checkpoints.poll();
            var lease = leases.get(next.id());

            if (lease == null) {
                continue;
            }

            // lease changed since scheduled
            if (next.renewalsAtSchedule() < lease.renewals()) {
                continue;
            }

            // epoch didn't change since schedule
            batch.add(new Checkpoint(next.id(), lease.remainingTtl() - config.checkpointInterval().toSeconds()));

            if (batch.size() == config.checkpointBatchSize()) {
                checkpointsBatch.add(batch);
                batch = new ArrayList<>();
            }
        }

        if (!batch.isEmpty() && batch.size() < config.checkpointBatchSize()) {
            checkpointsBatch.add(batch);
        }

        return checkpointsBatch;
    }
}
