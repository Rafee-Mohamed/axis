package io.disys.axis.wal;

import java.util.List;
import java.util.concurrent.BlockingQueue;

interface WriteBatcher {
    List<AsyncWal.PendingWrite> next(BlockingQueue<AsyncWal.PendingWrite> writes) throws InterruptedException;
}
