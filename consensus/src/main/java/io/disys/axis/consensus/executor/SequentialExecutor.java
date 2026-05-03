package io.disys.axis.consensus.executor;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.log.SequentialRaftLog;
import io.disys.axis.consensus.model.RaftPayload;
import io.disys.axis.consensus.proto.Command;
import io.disys.axis.consensus.proto.LeaseCheckpoint;
import io.disys.axis.consensus.sm.NodeContext;
import io.disys.axis.consensus.sm.StoreReader;
import io.disys.axis.consensus.sm.StoreWriter;
import io.disys.axis.consensus.transport.PeerTransport;
import io.disys.axis.lease.store.LeaseStore;
import io.disys.axis.mvcc.store.VersionedStore;
import io.disys.jaft.engine.VolatileState;
import io.disys.jaft.node.Node;
import io.disys.jaft.node.NodeLifecycleException;
import io.disys.jaft.node.task.ApplyTask;
import io.disys.jaft.node.task.WorkItem;
import io.disys.jaft.protocol.role.RoleType;
import io.disys.jaft.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class SequentialExecutor {

    private static final Logger log = LoggerFactory.getLogger(SequentialExecutor.class);
    private static final Duration DEFAULT_SCHEDULE_WAIT = Duration.ofSeconds(30);

    private final Node<RaftPayload, Long> node;
    private final SequentialRaftLog raftLog;
    private final VersionedStore store;
    private final LeaseStore leaseStore;
    private final PeerTransport transport;
    private final Map<Long, Object> responses;
    private final UIdGenerator uid;
    private final StoreReader storeReader;
    private final StoreWriter storeWriter;
    private final Duration tickInterval;
    private final long memberId;

    private volatile VolatileState volatileState;
    private volatile long term;

    // only accessed from the execution loop thread
    private long appliedIndex;
    private volatile Thread tickerThread;

    public SequentialExecutor(
            Node<RaftPayload, Long> node,
            SequentialRaftLog raftLog,
            VersionedStore store,
            LeaseStore leaseStore,
            PeerTransport transport,
            UIdGenerator uid,
            long clusterId,
            long memberId,
            Duration tickInterval
    ) throws IOException {
        this.node = node;
        this.raftLog = raftLog;
        this.store = store;
        this.leaseStore = leaseStore;
        this.transport = transport;
        this.uid = uid;
        this.tickInterval = tickInterval;
        this.memberId = memberId;
        this.responses = new ConcurrentHashMap<>();
        this.volatileState = new VolatileState(RoleType.FOLLOWER, Optional.empty());
        try {
            this.term = raftLog.initialState().persistentState().term();
        } catch (StorageException e) {
            throw new IOException("Failed to read initial raft state", e);
        }
        var nodeContext = new NodeContext(clusterId, memberId, () -> this.term);
        this.storeReader = new StoreReader(nodeContext);
        this.storeWriter = new StoreWriter(nodeContext, leaseStore, store);
    }

    // ===================== Lifecycle =====================

    public void start() {
        log.info("starting member={} term={} appliedIndex={}", memberId, term, appliedIndex);
        Thread.ofVirtual().name("raft-node").start(this::runNode);
        tickerThread = Thread.ofVirtual().name("raft-ticker").start(this::runTicker);
        Thread.ofVirtual().name("raft-exec").start(this::runLoop);
    }

    public void stop() throws InterruptedException {
        log.info("stopping member={}", memberId);
        node.shutdown();
        var t = tickerThread;
        if (t != null) t.interrupt();
    }

    // ===================== Execution loop =====================

    private void runNode() {
        try {
            node.run();
        } catch (NodeLifecycleException.GracefulShutdown _) {
            log.info("raft node shut down gracefully member={}", memberId);
        } catch (NodeLifecycleException.Termination t) {
            log.error("raft node terminated with error member={}", memberId, t);
        }
    }

    private void runTicker() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(tickInterval);
                node.tick();
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    private void runLoop() {
        log.info("execution loop started member={}", memberId);
        try {
            while (true) {
                var timeout = leaseStore.nextSchedule().orElse(DEFAULT_SCHEDULE_WAIT);
                var item = node.pollWork(timeout);
                if (item.isPresent()) {
                    switch (item.get()) {
                        case WorkItem.Terminated<RaftPayload> _ -> {
                            log.info("execution loop terminated member={}", memberId);
                            return;
                        }
                        case WorkItem.Work<RaftPayload> work -> processWork(work);
                    }
                } else if (volatileState.role() == RoleType.LEADER) {
                    processLeaseSchedule();
                }
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            log.warn("execution loop interrupted member={}", memberId);
        } catch (IOException e) {
            log.error("fatal error in execution loop member={}", memberId, e);
        }
    }

    private void processLeaseSchedule() throws InterruptedException {
        var expired = leaseStore.expired();
        for (var id : expired) {
            log.debug("lease expired leaseId={} member={}", id, memberId);
            var payload = new RaftPayload(nextCommand()
                    .setLeaseRevoke(RevokeRequest.newBuilder().setLeaseId(id).build())
                    .build());
            node.propose(payload).thenAccept(_ -> responses.remove(payload.id()));
        }
        var checkpoints = leaseStore.checkpoints();
        for (var batch : checkpoints) {
            var builder = LeaseCheckpoint.newBuilder();
            int count = 0;
            for (var checkpoint : batch) {
                builder.addEntries(LeaseCheckpoint.Entry.newBuilder()
                        .setLeaseId(checkpoint.id())
                        .setRemainingTtl(checkpoint.remainingTtl())
                        .build());
                count++;
            }
            log.debug("proposing lease checkpoint count={} member={}", count, memberId);
            node.propose(new RaftPayload(nextCommand()
                    .setLeaseCheckpoint(builder.build())
                    .build()));
        }
    }

    private void processWork(WorkItem.Work<RaftPayload> work) throws IOException, InterruptedException {
        work.volatileState().ifPresent(this::onVolatileStateChange);

        var outbound = work.messages();
        if (!outbound.isEmpty()) {
            log.debug("sending messages count={} member={}", outbound.size(), memberId);
            outbound.forEach(transport::send);
        }

        var persistTask = work.persistTask();
        if (persistTask.isPresent()) {
            var task = persistTask.get();
            int entryCount = task.entriesToPersist().size();
            boolean termChanged = task.persistentState().isPresent();
            log.debug("persisting entries={} termChanged={} member={}", entryCount, termChanged, memberId);
            raftLog.persist(task);
            var afterPersist = task.messagesAfterPersist();
            if (!afterPersist.isEmpty()) {
                log.debug("sending post-persist messages count={} member={}", afterPersist.size(), memberId);
                afterPersist.forEach(transport::send);
            }
            task.persistentState().ifPresent(ps -> {
                log.debug("term updated from={} to={} member={}", term, ps.term(), memberId);
                this.term = ps.term();
            });
            task.complete();
        }

        var applyTask = work.applyTask();
        if (applyTask.isPresent()) {
            applyEntries(applyTask.get());
        }
    }

    private void onVolatileStateChange(VolatileState next) {
        log.info("role from={} to={} term={} leader={} member={}",
                volatileState.role(), next.role(), term,
                next.leaderId().map(Object::toString).orElse("none"),
                memberId);
        var wasLeader = this.volatileState.role() == RoleType.LEADER;
        var isLeader = next.role() == RoleType.LEADER;
        this.volatileState = next;
        if (!wasLeader && isLeader) {
            log.info("became leader term={} member={}", term, memberId);
            leaseStore.trackSchedules();
        } else if (wasLeader && !isLeader) {
            log.info("stepped down from leader term={} member={}", term, memberId);
            leaseStore.untrackSchedules();
        }
    }

    private void applyEntries(ApplyTask<RaftPayload> task) throws IOException {
        var entries = task.entriesToApply();
        for (var payload : entries) {
            var cmd = payload.command();
            var cmdType = cmd.getTypeCase().name();
            log.debug("applying cmd={} cmdId={} member={}", cmdType, cmd.getCommandId(), memberId);
            try (var writer = store.writer()) {
                var result = storeWriter.apply(writer, cmd);
                if (result != null) {
                    responses.put(cmd.getCommandId(), result);
                }
            }
        }
        long prevApplied = appliedIndex;
        appliedIndex = task.lastIndex();
        if (!entries.isEmpty()) {
            log.debug("applied count={} appliedIndex={} prevApplied={} member={}",
                    entries.size(), appliedIndex, prevApplied, memberId);
        }
        task.complete();
    }

    // ===================== Reads =====================

    private <T> CompletableFuture<T> read(Function<io.disys.axis.mvcc.store.Reader, T> fn) throws InterruptedException {
        return node.readIndex().thenApply(_ -> {
            try (var r = store.reader()) {
                return fn.apply(r);
            }
        });
    }

    public CompletableFuture<GetResponse> get(GetRequest req) throws InterruptedException {
        log.debug("read op=GET member={}", memberId);
        return read(r -> storeReader.get(r, req));
    }

    public CompletableFuture<GetAtResponse> getAt(GetAtRequest req) throws InterruptedException {
        log.debug("read op=GET_AT member={}", memberId);
        return read(r -> storeReader.getAt(r, req));
    }

    public CompletableFuture<RangeResponse> range(RangeRequest req) throws InterruptedException {
        log.debug("read op=RANGE member={}", memberId);
        return read(r -> storeReader.range(r, req));
    }

    public CompletableFuture<RangeAtResponse> rangeAt(RangeAtRequest req) throws InterruptedException {
        log.debug("read op=RANGE_AT member={}", memberId);
        return read(r -> storeReader.rangeAt(r, req));
    }

    public CompletableFuture<KeysResponse> keys(KeysRequest req) throws InterruptedException {
        log.debug("read op=KEYS member={}", memberId);
        return read(r -> storeReader.keys(r, req));
    }

    public CompletableFuture<KeysAtResponse> keysAt(KeysAtRequest req) throws InterruptedException {
        log.debug("read op=KEYS_AT member={}", memberId);
        return read(r -> storeReader.keysAt(r, req));
    }

    public CompletableFuture<CountResponse> count(CountRequest req) throws InterruptedException {
        log.debug("read op=COUNT member={}", memberId);
        return read(r -> storeReader.count(r, req));
    }

    public CompletableFuture<CountAtResponse> countAt(CountAtRequest req) throws InterruptedException {
        log.debug("read op=COUNT_AT member={}", memberId);
        return read(r -> storeReader.countAt(r, req));
    }

    // ===================== Writes =====================

    private <T> CompletableFuture<T> propose(Command cmd) throws InterruptedException {
        log.debug("propose cmd={} cmdId={} member={}", cmd.getTypeCase().name(), cmd.getCommandId(), memberId);
        var payload = new RaftPayload(cmd);
        return node.propose(payload)
                .thenApply(_ -> {
                    @SuppressWarnings("unchecked")
                    T response = (T) responses.remove(payload.id());
                    if (response == null) {
                        log.warn("propose completed with null response cmd={} cmdId={} member={}",
                                cmd.getTypeCase().name(), cmd.getCommandId(), memberId);
                    }
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

    public CompletableFuture<InfoResponse> leaseInfo(InfoRequest request) throws InterruptedException {
        log.debug("read op=LEASE_INFO leaseId={} member={}", request.getLeaseId(), memberId);
        return node.readIndex().thenApply(_ ->
                leaseStore.leaseInfo(request.getLeaseId())
                        .map(s -> InfoResponse.newBuilder()
                                .setFound(LeaseDetail.newBuilder()
                                        .setLeaseId(s.id())
                                        .setTtl(s.ttl())
                                        .setRemainingTtl(s.remainingTtl())
                                        .addAllKeys(s.keys().stream()
                                                .map(com.google.protobuf.ByteString::copyFrom)
                                                .toList())
                                        .build())
                                .build())
                        .orElseGet(() -> InfoResponse.newBuilder()
                                .setNotFound(LeaseNotFound.newBuilder()
                                        .setLeaseId(request.getLeaseId())
                                        .build())
                                .build())
        );
    }

    public CompletableFuture<LeasesResponse> leaseList(LeasesRequest request) throws InterruptedException {
        log.debug("read op=LEASE_LIST member={}", memberId);
        return node.readIndex().thenApply(_ -> {
            var infos = leaseStore.leaseList().stream()
                    .map(s -> LeaseInfo.newBuilder()
                            .setLeaseId(s.id())
                            .setTtl(s.ttl())
                            .setRemainingTtl(s.remainingTtl())
                            .build())
                    .toList();
            return LeasesResponse.newBuilder().addAllLeases(infos).build();
        });
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
