package io.disys.axis.consensus.executor;

import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.model.RaftPayload;
import io.disys.axis.consensus.proto.Command;
import io.disys.axis.consensus.transport.PeerTransport;
import io.disys.axis.lease.store.LeaseStore;
import io.disys.axis.mvcc.store.Reader;
import io.disys.axis.mvcc.store.VersionedStore;
import io.disys.axis.wal.api.Wal;
import io.disys.jaft.engine.VolatileState;
import io.disys.jaft.node.Node;
import io.disys.jaft.protocol.role.RoleType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class SequentialExecutor {
    private final Node<RaftPayload, Long> node;
    private final Wal wal;
    private final VersionedStore store;
    private final LeaseStore leaseStore;
    private final PeerTransport transport;
    private final Map<Long, Object> responses;
    private final UIdGenerator uid;
    private volatile VolatileState volatileState;

    public SequentialExecutor(
            Node<RaftPayload, Long> node,
            Wal wal,
            VersionedStore store,
            LeaseStore leaseStore,
            PeerTransport transport,
            UIdGenerator uid
    ) {
        this.node = node;
        this.wal = wal;
        this.store = store;
        this.leaseStore = leaseStore;
        this.transport = transport;
        this.responses = new ConcurrentHashMap<>();
        this.uid = uid;
        this.volatileState = new VolatileState(RoleType.FOLLOWER, Optional.empty());
    }

    // ===================== Reads =====================

    private <T> CompletableFuture<T> read(Function<Reader, T> fn) throws InterruptedException {
        return node.readIndex().thenApply(_ -> {
            try (var r = store.reader()) {
                return fn.apply(r);
            }
        });
    }

    public CompletableFuture<GetResponse> get(GetRequest req) throws InterruptedException {
        return read(r -> StoreReader.get(r, req));
    }

    public CompletableFuture<GetAtResponse> getAt(GetAtRequest req) throws InterruptedException {
        return read(r -> StoreReader.getAt(r, req));
    }

    public CompletableFuture<RangeResponse> range(RangeRequest req) throws InterruptedException {
        return read(r -> StoreReader.range(r, req));
    }

    public CompletableFuture<RangeAtResponse> rangeAt(RangeAtRequest req) throws InterruptedException {
        return read(r -> StoreReader.rangeAt(r, req));
    }

    public CompletableFuture<KeysResponse> keys(KeysRequest req) throws InterruptedException {
        return read(r -> StoreReader.keys(r, req));
    }

    public CompletableFuture<KeysAtResponse> keysAt(KeysAtRequest req) throws InterruptedException {
        return read(r -> StoreReader.keysAt(r, req));
    }

    public CompletableFuture<CountResponse> count(CountRequest req) throws InterruptedException {
        return read(r -> StoreReader.count(r, req));
    }

    public CompletableFuture<CountAtResponse> countAt(CountAtRequest req) throws InterruptedException {
        return read(r -> StoreReader.countAt(r, req));
    }

    // ===================== Writes =====================

    private <T> CompletableFuture<T> propose(Command cmd) throws InterruptedException {
        var payload = new RaftPayload(cmd);
        return node.propose(payload)
                .thenApply(_ -> {
                    @SuppressWarnings("unchecked")
                    T response = (T) responses.remove(payload.id());
                    return response;
                });
    }

    private Command.Builder nextCommand() {
        return Command.newBuilder().setCommandId(uid.next());
    }

    public CompletableFuture<PutResponse> put(PutRequest request) throws InterruptedException {
        return propose(nextCommand().setPut(request).build());
    }

    public CompletableFuture<PutAndGetResponse> putAndGet(PutAndGetRequest request) throws InterruptedException {
        return propose(nextCommand().setPutAndGet(request).build());
    }

    public CompletableFuture<PutResponse> putWithLease(PutWithLeaseRequest request) throws InterruptedException {
        return propose(nextCommand().setPutWithLease(request).build());
    }

    public CompletableFuture<PutAndGetResponse> putWithLeaseAndGet(PutWithLeaseAndGetRequest request) throws InterruptedException {
        return propose(nextCommand().setPutWithLeaseAndGet(request).build());
    }

    public CompletableFuture<PutResponse> updateLease(UpdateLeaseRequest request) throws InterruptedException {
        return propose(nextCommand().setUpdateLease(request).build());
    }

    public CompletableFuture<PutResponse> updateValue(UpdateValueRequest request) throws InterruptedException {
        return propose(nextCommand().setUpdateValue(request).build());
    }

    public CompletableFuture<PutResponse> removeLease(RemoveLeaseRequest request) throws InterruptedException {
        return propose(nextCommand().setRemoveLease(request).build());
    }

    public CompletableFuture<DeleteResponse> delete(DeleteRequest request) throws InterruptedException {
        return propose(nextCommand().setDelete(request).build());
    }

    public CompletableFuture<DeleteAndGetResponse> deleteAndGet(DeleteAndGetRequest request) throws InterruptedException {
        return propose(nextCommand().setDeleteAndGet(request).build());
    }

    public CompletableFuture<DeleteRangeResponse> deleteRange(DeleteRangeRequest request) throws InterruptedException {
        return propose(nextCommand().setDeleteRange(request).build());
    }

    public CompletableFuture<DeleteRangeAndGetResponse> deleteRangeAndGet(DeleteRangeAndGetRequest request) throws InterruptedException {
        return propose(nextCommand().setDeleteRangeAndGet(request).build());
    }

    public CompletableFuture<TxnResponse> txn(TxnRequest request) throws InterruptedException {
        return propose(nextCommand().setTxn(request).build());
    }

    public CompletableFuture<CompactResponse> compact(CompactRequest request) throws InterruptedException {
        return propose(nextCommand().setCompact(request).build());
    }

    public CompletableFuture<GrantResponse> leaseGrant(GrantRequest request) throws InterruptedException {
        var populated = request.getLeaseId() == 0
                ? request.toBuilder().setLeaseId(uid.next()).build()
                : request;
        return propose(nextCommand().setLeaseGrant(populated).build());
    }

    public CompletableFuture<RevokeResponse> leaseRevoke(RevokeRequest request) throws InterruptedException {
        return propose(nextCommand().setLeaseRevoke(request).build());
    }
}
