package io.disys.axis.server;

import io.disys.axis.backend.lmdb.LmdbConfig;
import io.disys.axis.lease.store.LeaseStoreConfig;
import io.disys.axis.mvcc.store.TimelineVersionedStoreConfig;
import io.disys.axis.wal.api.WalConfig;
import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.config.RaftConfig;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.node.NodeConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

public record ServerConfig(
        long clusterId,
        long memberId,
        int clientPort,
        int peerPort,
        Map<NodeId, InetSocketAddress> peers,
        MembershipConfig initialMembership,
        Duration tickInterval,
        LmdbConfig lmdb,
        WalConfig wal,
        TimelineVersionedStoreConfig mvcc,
        LeaseStoreConfig lease,
        RaftConfig raft,
        NodeConfig node
) {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long clusterId;
        private long memberId;
        private int clientPort;
        private int peerPort;
        private Map<NodeId, InetSocketAddress> peers = Map.of();
        private MembershipConfig initialMembership;
        private Duration tickInterval = Duration.ofMillis(100);
        private LmdbConfig lmdb;
        private WalConfig wal;
        private TimelineVersionedStoreConfig mvcc;
        private LeaseStoreConfig lease;
        private RaftConfig raft;
        private NodeConfig node;

        public Builder clusterId(long v)               { clusterId = v; return this; }
        public Builder memberId(long v)                { memberId = v; return this; }
        public Builder clientPort(int v)               { clientPort = v; return this; }
        public Builder peerPort(int v)                 { peerPort = v; return this; }
        public Builder peers(Map<NodeId, InetSocketAddress> v) { peers = v; return this; }
        public Builder initialMembership(MembershipConfig v)   { initialMembership = v; return this; }
        public Builder tickInterval(Duration v)        { tickInterval = v; return this; }
        public Builder lmdb(LmdbConfig v)              { lmdb = v; return this; }
        public Builder wal(WalConfig v)                { wal = v; return this; }
        public Builder mvcc(TimelineVersionedStoreConfig v) { mvcc = v; return this; }
        public Builder lease(LeaseStoreConfig v)       { lease = v; return this; }
        public Builder raft(RaftConfig v)              { raft = v; return this; }
        public Builder node(NodeConfig v)              { node = v; return this; }

        public ServerConfig build() {
            if (initialMembership == null) throw new IllegalStateException("initialMembership required");
            if (lmdb == null) throw new IllegalStateException("lmdb required");
            if (wal == null) throw new IllegalStateException("wal required");
            if (mvcc == null) throw new IllegalStateException("mvcc required");
            if (lease == null) throw new IllegalStateException("lease required");
            if (raft == null) throw new IllegalStateException("raft required");
            if (node == null) throw new IllegalStateException("node required");
            return new ServerConfig(
                    clusterId, memberId, clientPort, peerPort, peers, initialMembership,
                    tickInterval, lmdb, wal, mvcc, lease, raft, node
            );
        }
    }
}
