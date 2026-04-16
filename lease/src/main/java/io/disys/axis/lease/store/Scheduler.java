package io.disys.axis.lease.store;

import io.disys.axis.lease.model.Checkpoint;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

public class Scheduler {
    private final PriorityQueue<LeaseInstant> checkpoints;
    private final PriorityQueue<LeaseInstant> deadlines;
    private final Duration checkpointInterval;
    private final Clock clock;


    public Scheduler(Clock clock, Duration checkpointInterval) {
        this.checkpoints = new PriorityQueue<>();
        this.deadlines = new PriorityQueue<>();
        this.clock = clock;
        this.checkpointInterval = checkpointInterval;
    }


    public static Scheduler fromLeases(Collection<Lease> leases, Clock clock, Duration checkpointInterval) {
        var scheduler = new Scheduler(clock, checkpointInterval);
        for (var lease: leases) {
            scheduler.schedule(lease);
        }
        return scheduler;
    }


    public void schedule(Lease lease) {
        deadlines.add(new LeaseInstant(lease.id(), lease.expiry(), lease.renewals()));

        if (lease.remainingTtl() <= checkpointInterval.toSeconds()) {
            return;
        }
        var nextCheckpoint = clock.instant().plus(checkpointInterval);
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

    public List<Long> expired(Map<Long, Lease> leases) {
        var expiredLeases = new ArrayList<Long>();
        var now = clock.instant();

        while (!deadlines.isEmpty() && deadlines.peek().when().isBefore(now)) {
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
        }

        return expiredLeases;
    }

    public List<Checkpoint> checkpoints(Map<Long, Lease> leases) {
        var nextCheckpoints = new ArrayList<Checkpoint>();
        var now = clock.instant();

        while (!checkpoints.isEmpty() && checkpoints.peek().when().isBefore(now)) {
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
            nextCheckpoints.add(new Checkpoint(next.id(), lease.remainingTtl() - checkpointInterval.toSeconds()));
        }

        return nextCheckpoints;
    }
}
