package io.disys.axis.wal.async;

import io.disys.axis.wal.api.AsyncWal;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public interface WriteBatcher {
    List<AsyncWal.PendingWrite> next(BlockingQueue<AsyncWal.PendingWrite> writes) throws InterruptedException;
}
