package io.disys.axis.wal.async;

import io.disys.axis.wal.api.AsyncWal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class OnDemandWriteBatcher implements WriteBatcher {

    @Override
    public List<AsyncWal.PendingWrite> next(BlockingQueue<AsyncWal.PendingWrite> writes) throws InterruptedException {
        var batch = new ArrayList<AsyncWal.PendingWrite>();
        var nextWrite = writes.take();
        batch.add(nextWrite);
        writes.drainTo(batch);
        return batch;
    }
}
