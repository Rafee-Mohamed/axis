package io.disys.axis.server;

import io.disys.axis.backend.Database;
import io.disys.axis.backend.lmdb.LmdbBackend;
import io.disys.axis.consensus.executor.SequentialExecutor;
import io.disys.axis.consensus.executor.UIdGenerator;
import io.disys.axis.consensus.log.SequentialRaftLog;
import io.disys.axis.consensus.model.RaftPayload;
import io.disys.axis.consensus.transport.PeerTransport;
import io.disys.axis.lease.store.LeaseStore;
import io.disys.axis.mvcc.store.TimelineVersionedStore;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.core.RaftState;
import io.disys.jaft.engine.ExecutionModel;
import io.disys.jaft.node.Node;
import io.disys.jaft.storage.StorageException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;

public final class AxisServer {

    private static final Logger log = LoggerFactory.getLogger(AxisServer.class);

    private final SequentialExecutor executor;
    private final PeerTransport peerTransport;
    private final Server clientServer;
    private final LmdbBackend backend;

    private AxisServer(SequentialExecutor executor, PeerTransport peerTransport, Server clientServer, LmdbBackend backend) {
        this.executor = executor;
        this.peerTransport = peerTransport;
        this.clientServer = clientServer;
        this.backend = backend;
    }

    public static AxisServer create(ServerConfig config) throws IOException, StorageException {
        if (config.raft().executionModel() != ExecutionModel.SEQUENTIAL) {
            throw new IllegalArgumentException("AxisServer requires RaftConfig.executionModel = SEQUENTIAL");
        }
        var requiredDbs = Set.of(
                config.mvcc().versionDB(), config.mvcc().metaDB(), config.lease().leaseDb()
        );
        var registeredDbs = config.lmdb().databases().stream()
                .map(Database::name)
                .collect(Collectors.toSet());
        if (!registeredDbs.containsAll(requiredDbs)) {
            var missing = new HashSet<>(requiredDbs);
            missing.removeAll(registeredDbs);
            throw new IllegalArgumentException("LmdbConfig is missing databases: " + missing);
        }

        var backend = new LmdbBackend(config.lmdb());

        var leaseStore = LeaseStore.restore(backend, config.lease(), Clock.systemUTC());

        var store = TimelineVersionedStore.restore(
                backend, config.mvcc(), leaseStore.keysRecoveryConsumer());

        Files.createDirectories(config.wal().directory());
        var raftLog = SequentialRaftLog.open(config.wal());

        var initialState = raftLog.initialState();
        var ps = initialState.persistentState();
        var raftState = new RaftState(
                new NodeId(config.memberId()),
                ps.term(),
                raftLog.committedIndex(),
                ps.votedFor()
        );

        var membership = initialState.membershipConfig().currentVoters().isEmpty()
                ? config.initialMembership()
                : initialState.membershipConfig();

        var node = new Node<RaftPayload, Long>(
                config.node(), raftState, config.raft(), raftLog, membership, RandomGenerator.getDefault()
        );

        var peerTransport = new PeerTransport(
                config.peerPort(), config.peers(), msg -> {
                    try {
                        node.receive(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
        );

        var uid = new UIdGenerator(config.memberId(), Instant.now().toEpochMilli());
        var executor = new SequentialExecutor(
                node, raftLog, store, leaseStore, peerTransport,
                uid, config.clusterId(), config.memberId(), config.tickInterval()
        );

        var clientServer = ServerBuilder.forPort(config.clientPort())
                .addService(new KvServiceImpl(executor))
                .addService(new LeaseServiceImpl(executor))
                .build();

        return new AxisServer(executor, peerTransport, clientServer, backend);
    }

    public void start() throws IOException {
        peerTransport.start();
        executor.start();
        clientServer.start();
        log.info("AxisServer started on port {}", clientServer.getPort());
    }

    public void stop() throws InterruptedException {
        clientServer.shutdown();
        executor.stop();
        peerTransport.shutdown();
        backend.close();
    }

    public void awaitTermination() throws InterruptedException {
        clientServer.awaitTermination();
    }
}
