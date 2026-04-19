package io.disys.axis.consensus.transport;

import io.disys.axis.consensus.transport.codec.PeerMessageCodec;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.message.Message;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class PeerTransport {

    private final Server server;
    private final Map<NodeId, PeerConnection> connections;

    public PeerTransport(int localPort, Map<NodeId, InetSocketAddress> peers, Consumer<Message.Peer> receiver) {
        this.server = ServerBuilder.forPort(localPort)
                .addService(new RaftTransportService(receiver))
                .build();

        var conn = new HashMap<NodeId, PeerConnection>();
        for (var entry : peers.entrySet()) {
            conn.put(entry.getKey(), new PeerConnection(
                    entry.getKey(),
                    entry.getValue().getHostString(),
                    entry.getValue().getPort(),
                    receiver
            ));
        }
        this.connections = conn;
    }

    public void start() throws IOException {
        server.start();
        connections.values().forEach(PeerConnection::connect);
    }

    public void send(Message.Peer message) {
        var conn = connections.get(extractTo(message));
        if (conn == null) return;
        conn.send(PeerMessageCodec.encode(message));
    }

    public void shutdown() {
        server.shutdownNow();
        connections.values().forEach(PeerConnection::shutdown);
    }

    private static NodeId extractTo(Message.Peer message) {
        return switch (message) {
            case Message.AppendEntries m          -> m.to();
            case Message.AppendEntriesResponse m  -> m.to();
            case Message.InstallSnapshot m        -> m.to();
            case Message.Heartbeat m              -> m.to();
            case Message.HeartbeatResponse m      -> m.to();
            case Message.RequestVote m            -> m.to();
            case Message.RequestVoteResponse m    -> m.to();
            case Message.RequestPreVote m         -> m.to();
            case Message.RequestPreVoteResponse m -> m.to();
            case Message.TransferLeadership m     -> m.to();
            case Message.TimeoutNow m             -> m.to();
            case Message.ReadIndex m              -> m.to();
            case Message.ReadIndexResponse m      -> m.to();
            case Message.DataProposal m           -> m.to();
            case Message.MembershipChangeProposal m -> m.to();
            case Message.LeaveJointProposal m     -> m.to();
        };
    }
}
