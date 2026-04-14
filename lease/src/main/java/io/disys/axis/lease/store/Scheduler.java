package io.disys.axis.lease.store;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.PriorityQueue;

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

    public void schedule(Lease lease) {
        deadlines.add(new LeaseInstant(lease.id(), lease.expiry(), lease.scheduleEpoch()));

        if (lease.remainingTtl() <= checkpointInterval.toSeconds()) {
            return;
        }
        var nextCheckpoint = clock.instant().plus(checkpointInterval);
        checkpoints.add(new LeaseInstant(lease.id(), nextCheckpoint, lease.scheduleEpoch()));
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
}
