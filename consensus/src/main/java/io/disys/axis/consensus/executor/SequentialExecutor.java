package io.disys.axis.consensus.executor;

import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.model.RaftPayload;
import io.disys.axis.consensus.proto.Command;
import io.disys.axis.consensus.proto.PutCommand;
import io.disys.axis.consensus.transport.PeerTransport;
import io.disys.axis.lease.store.LeaseStore;
import io.disys.axis.mvcc.store.VersionedStore;
import io.disys.axis.wal.api.Wal;
import io.disys.jaft.engine.VolatileState;
import io.disys.jaft.node.Node;
import io.disys.jaft.protocol.role.RoleType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

public final class SequentialExecutor {
    private final Node<RaftPayload, Long> node;
    private final Wal wal;
    private final VersionedStore store;
    private final StoreReader reader;
    private final LeaseStore leaseStore;
    private final PeerTransport transport;
    private final Map<Long, Object> responses;
    private final CommandIdGenerator idGen;
    private volatile VolatileState volatileState;

    public SequentialExecutor(
            Node<RaftPayload, Long> node,
            Wal wal,
            VersionedStore store,
            LeaseStore leaseStore,
            PeerTransport transport,
            CommandIdGenerator idGen
    ) {
        this.node = node;
        this.wal = wal;
        this.store = store;
        this.leaseStore = leaseStore;
        this.transport = transport;
        this.responses = new ConcurrentHashMap<>();
        this.idGen = idGen;
        this.reader = new StoreReader(store);
        this.volatileState = new VolatileState(RoleType.FOLLOWER, Optional.empty());
    }

    // ===================== Reads =====================

    public CompletableFuture<GetResponse> get(GetRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.get(req));
    }

    public CompletableFuture<GetAtResponse> getAt(GetAtRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.getAt(req));
    }

    public CompletableFuture<RangeResponse> range(RangeRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.range(req));
    }

    public CompletableFuture<RangeAtResponse> rangeAt(RangeAtRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.rangeAt(req));
    }

    public CompletableFuture<KeysResponse> keys(KeysRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.keys(req));
    }

    public CompletableFuture<KeysAtResponse> keysAt(KeysAtRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.keysAt(req));
    }

    public CompletableFuture<CountResponse> count(CountRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.count(req));
    }

    public CompletableFuture<CountAtResponse> countAt(CountAtRequest req) throws InterruptedException {
        return node.readIndex().thenApply(_ -> reader.countAt(req));
    }

    // ===================== Writes =====================

    public CompletableFuture<PutResponse> put(PutRequest request) throws InterruptedException {
        var put = PutCommand.newBuilder()
                .setKey(request.getKey())
                .setVal(KeyValCodec.encode(request.getVal()));

        var cmd = Command.newBuilder()
                .setCommandId(idGen.next())
                .setPut(put)
                .build();

        return node.propose(new RaftPayload(cmd))
                .thenApply(_ -> (PutResponse) responses.remove(cmd.getCommandId()));
    }

    public CompletableFuture<PutAndGetResponse> putAndGet(PutAndGetRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<PutResponse> putWithLease(PutWithLeaseRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<PutAndGetResponse> putWithLeaseAndGet(PutWithLeaseAndGetRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<PutResponse> updateLease(UpdateLeaseRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<PutResponse> updateValue(UpdateValueRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<PutResponse> removeLease(RemoveLeaseRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<DeleteResponse> delete(DeleteRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<DeleteAndGetResponse> deleteAndGet(DeleteAndGetRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<DeleteRangeResponse> deleteRange(DeleteRangeRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<DeleteRangeAndGetResponse> deleteRangeAndGet(DeleteRangeAndGetRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<TxnResponse> txn(TxnRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<CompactResponse> compact(CompactRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<GrantResponse> leaseGrant(GrantRequest request) { throw new UnsupportedOperationException(); }

    public CompletableFuture<RevokeResponse> leaseRevoke(RevokeRequest request) { throw new UnsupportedOperationException(); }
}
