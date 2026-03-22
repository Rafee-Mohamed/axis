package io.disys.axis.wal.api;

import io.disys.axis.wal.async.AsyncWalState;
import io.disys.axis.wal.async.OnDemandWriteBatcher;
import io.disys.axis.wal.async.PeriodicWriteBatcher;
import io.disys.axis.wal.async.WriteBatcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncWal {
    public record PendingWrite(List<ByteBuffer> payload, CompletableFuture<Void> future) {}

    private final PendingWrite POISON = new PendingWrite(List.of(), new CompletableFuture<>());
    private final Wal wal;
    private final BlockingQueue<PendingWrite> writes;
    private final AtomicReference<AsyncWalState> state;
    private final WriteBatcher batcher;

    public AsyncWal(Wal wal, AsyncWalConfig config) {
        this.wal = wal;
        this.state =  new AtomicReference<>(AsyncWalState.CREATED);
        this.writes = new ArrayBlockingQueue<>(config.maxPendingWrites());
        this.batcher = switch (config.flushStrategy()) {
            case FlushStrategy.OnDemand _ -> new OnDemandWriteBatcher();
            case FlushStrategy.Periodic(var interval) -> new PeriodicWriteBatcher(interval);
        };
    }

    public CompletableFuture<Void> append(List<ByteBuffer> payload) throws InterruptedException {
        var currentAsyncWalState = state.get();
        if (currentAsyncWalState != AsyncWalState.RUNNING) {
            return CompletableFuture.failedFuture(
                    new AsyncWalException.UnexpectedState(AsyncWalState.RUNNING, currentAsyncWalState)
            );
        }

        var failedFuture = checkPayload(payload);
        if (failedFuture != null) {
            return failedFuture;
        }

        var future = new CompletableFuture<Void>();
        writes.put(new PendingWrite(payload, future));
        return future;
    }

    private CompletableFuture<Void> checkPayload(List<ByteBuffer> payload) {
        try {
            wal.validateRecordSize(payload);
            return null;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public AsyncWalState close() throws InterruptedException {
        if (state.compareAndSet(AsyncWalState.RUNNING, AsyncWalState.CLOSING)) {
            writes.put(POISON);
        }
        return state.get();
    }

    private void drainAndFail(Throwable t) {
        var batch = new ArrayList<PendingWrite>();
        writes.drainTo(batch);
        failWrites(batch, t);
    }

    private void failWrites(List<PendingWrite> writes, Throwable t) {
        writes.forEach(write -> write.future().completeExceptionally(t));
    }

    private void tryCloseWal() {
        try {
            wal.close();
        } catch (IOException e) {
            // tried to close but failed
        }
    }

    private void fail(List<PendingWrite> failedWrites, Throwable t) throws AsyncWalException.Terminated {
        state.set(AsyncWalState.FAILED);
        failWrites(failedWrites, t);
        drainAndFail(t);
        tryCloseWal();
        throw new AsyncWalException.Terminated(t);

    }

    private void completeWrites(List<PendingWrite> writes) {
        writes.forEach(write -> write.future().complete(null));
    }

    private void performWrites(List<PendingWrite> writes) throws IOException {
        var payloadBatch = writes.stream()
                .<ByteBuffer>mapMulti(
                        (write, consumer) -> write.payload().forEach(consumer))
                .toList();

        wal.appendValid(payloadBatch);
        completeWrites(writes);
    }

    public void run() throws AsyncWalException {
        if (!state.compareAndSet(AsyncWalState.CREATED, AsyncWalState.RUNNING)) {
            throw new AsyncWalException.UnexpectedState(AsyncWalState.CREATED, state.get());
        }

        while (state.get() == AsyncWalState.RUNNING || state.get() == AsyncWalState.CLOSING) {
            List<PendingWrite> batch = List.of();
            try {
                batch = batcher.next(writes);

                var poisonIndex = batch.indexOf(POISON);

                if (poisonIndex != -1) {
                    var writesToFail = batch.subList(poisonIndex + 1, batch.size());
                    // poison intercepted successfully
                    batch.get(poisonIndex).future().complete(null);
                    batch = batch.subList(0, poisonIndex);
                    failWrites(writesToFail, new AsyncWalException.UnexpectedState(AsyncWalState.RUNNING, state.get()));
                    performWrites(batch);
                    tryCloseWal();
                    state.set(AsyncWalState.CLOSED);
                    throw new AsyncWalException.GracefulShutdown();
                }

                performWrites(batch);
            } catch (AsyncWalException.GracefulShutdown e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(batch, e);
            } catch (Exception e) {
                fail(batch, e);
            }
        }

        throw new AssertionError("unreachable");
    }
}
