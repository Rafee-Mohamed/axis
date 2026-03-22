package io.disys.axis.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

class PeriodicWriteBatcher implements WriteBatcher {
    private final long interval;
    private long nextWriteTime;

    PeriodicWriteBatcher(long interval) {
        this.interval = TimeUnit.MILLISECONDS.toNanos(interval);
        this.nextWriteTime = System.nanoTime() + this.interval;
    }

    private void park() throws InterruptedException {
        while (true) {
            var remainingWaitTime = nextWriteTime - System.nanoTime();
            if (remainingWaitTime <= 0) break;
            if (Thread.interrupted()) throw new InterruptedException();
            LockSupport.parkNanos(remainingWaitTime);
        }
    }


    @Override
    public List<AsyncWal.PendingWrite> next(BlockingQueue<AsyncWal.PendingWrite> writes) throws InterruptedException {
        var batch = new ArrayList<AsyncWal.PendingWrite>();

        while (batch.isEmpty()) {
            park();
            writes.drainTo(batch);
            while (nextWriteTime < System.nanoTime()) {
                nextWriteTime += interval;
            }
        }

        return batch;
    }
}
