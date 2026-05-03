package io.disys.axis.server;

import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.executor.SequentialExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

final class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final SequentialExecutor executor;

    KvServiceImpl(SequentialExecutor executor) {
        this.executor = executor;
    }

    // ===================== Reads =====================

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> out) {
        handle(out, () -> executor.get(req));
    }

    @Override
    public void getAt(GetAtRequest req, StreamObserver<GetAtResponse> out) {
        handle(out, () -> executor.getAt(req));
    }

    @Override
    public void range(RangeRequest req, StreamObserver<RangeResponse> out) {
        handle(out, () -> executor.range(req));
    }

    @Override
    public void rangeAt(RangeAtRequest req, StreamObserver<RangeAtResponse> out) {
        handle(out, () -> executor.rangeAt(req));
    }

    @Override
    public void keys(KeysRequest req, StreamObserver<KeysResponse> out) {
        handle(out, () -> executor.keys(req));
    }

    @Override
    public void keysAt(KeysAtRequest req, StreamObserver<KeysAtResponse> out) {
        handle(out, () -> executor.keysAt(req));
    }

    @Override
    public void count(CountRequest req, StreamObserver<CountResponse> out) {
        handle(out, () -> executor.count(req));
    }

    @Override
    public void countAt(CountAtRequest req, StreamObserver<CountAtResponse> out) {
        handle(out, () -> executor.countAt(req));
    }

    // ===================== Writes =====================

    @Override
    public void put(PutRequest req, StreamObserver<PutResponse> out) {
        handle(out, () -> executor.put(req));
    }

    @Override
    public void putAndGet(PutAndGetRequest req, StreamObserver<PutAndGetResponse> out) {
        handle(out, () -> executor.putAndGet(req));
    }

    @Override
    public void putWithLease(PutWithLeaseRequest req, StreamObserver<PutWithLeaseResponse> out) {
        handle(out, () -> executor.putWithLease(req));
    }

    @Override
    public void putWithLeaseAndGet(PutWithLeaseAndGetRequest req, StreamObserver<PutWithLeaseAndGetResponse> out) {
        handle(out, () -> executor.putWithLeaseAndGet(req));
    }

    @Override
    public void updateLease(UpdateLeaseRequest req, StreamObserver<UpdateLeaseResponse> out) {
        handle(out, () -> executor.updateLease(req));
    }

    @Override
    public void updateValue(UpdateValueRequest req, StreamObserver<UpdateValueResponse> out) {
        handle(out, () -> executor.updateValue(req));
    }

    @Override
    public void removeLease(RemoveLeaseRequest req, StreamObserver<PutResponse> out) {
        handle(out, () -> executor.removeLease(req));
    }

    @Override
    public void delete(DeleteRequest req, StreamObserver<DeleteResponse> out) {
        handle(out, () -> executor.delete(req));
    }

    @Override
    public void deleteAndGet(DeleteAndGetRequest req, StreamObserver<DeleteAndGetResponse> out) {
        handle(out, () -> executor.deleteAndGet(req));
    }

    @Override
    public void deleteRange(DeleteRangeRequest req, StreamObserver<DeleteRangeResponse> out) {
        handle(out, () -> executor.deleteRange(req));
    }

    @Override
    public void deleteRangeAndGet(DeleteRangeAndGetRequest req, StreamObserver<DeleteRangeAndGetResponse> out) {
        handle(out, () -> executor.deleteRangeAndGet(req));
    }

    @Override
    public void txn(TxnRequest req, StreamObserver<TxnResponse> out) {
        handle(out, () -> executor.txn(req));
    }

    @Override
    public void compact(CompactRequest req, StreamObserver<CompactResponse> out) {
        handle(out, () -> executor.compact(req));
    }

    // ===================== Helper =====================

    private <T> void handle(StreamObserver<T> out, InterruptedSupplier<CompletableFuture<T>> call) {
        try {
            call.get().whenComplete((result, ex) -> {
                if (ex != null) {
                    out.onError(Status.INTERNAL.withCause(ex).asRuntimeException());
                } else {
                    out.onNext(result);
                    out.onCompleted();
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.onError(Status.UNAVAILABLE.withDescription("interrupted").asRuntimeException());
        }
    }

    @FunctionalInterface
    private interface InterruptedSupplier<T> {
        T get() throws InterruptedException;
    }
}
