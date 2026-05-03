package io.disys.axis.server;

import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.executor.SequentialExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

final class LeaseServiceImpl extends LeaseServiceGrpc.LeaseServiceImplBase {

    private final SequentialExecutor executor;

    LeaseServiceImpl(SequentialExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void grant(GrantRequest req, StreamObserver<GrantResponse> out) {
        handle(out, () -> executor.leaseGrant(req));
    }

    @Override
    public void revoke(RevokeRequest req, StreamObserver<RevokeResponse> out) {
        handle(out, () -> executor.leaseRevoke(req));
    }

    @Override
    public void info(InfoRequest req, StreamObserver<InfoResponse> out) {
        handle(out, () -> executor.leaseInfo(req));
    }

    @Override
    public void leases(LeasesRequest req, StreamObserver<LeasesResponse> out) {
        handle(out, () -> executor.leaseList(req));
    }

    @Override
    public StreamObserver<RenewRequest> renew(StreamObserver<RenewResponse> out) {
        out.onError(Status.UNIMPLEMENTED.withDescription("Renew not yet implemented").asRuntimeException());
        return new StreamObserver<>() {
            @Override public void onNext(RenewRequest value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
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
