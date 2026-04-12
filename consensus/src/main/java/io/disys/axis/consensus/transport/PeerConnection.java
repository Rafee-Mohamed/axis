package io.disys.axis.consensus.transport;

import io.disys.axis.consensus.proto.PeerMessage;
import io.disys.axis.consensus.proto.RaftTransportServiceGrpc;
import io.disys.axis.consensus.transport.codec.PeerMessageCodec;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.message.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public final class PeerConnection {

    private static final Logger log = LoggerFactory.getLogger(PeerConnection.class);

    private final NodeId peerId;
    private final ManagedChannel channel;
    private final Consumer<Message.Peer> receiver;
    private volatile StreamObserver<PeerMessage> sender;

    public PeerConnection(NodeId peerId, String host, int port, Consumer<Message.Peer> receiver) {
        this.peerId = peerId;
        this.receiver = receiver;
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public void connect() {
        var stub = RaftTransportServiceGrpc.newStub(channel);
        sender = stub.stream(new StreamObserver<>() {

            @Override
            public void onNext(PeerMessage msg) {
                receiver.accept(PeerMessageCodec.decode(msg));
            }

            @Override
            public void onError(Throwable t) {
                log.warn("Stream to peer {} lost", peerId);
                sender = null;
            }

            @Override
            public void onCompleted() {
                sender = null;
            }
        });
    }

    public void send(PeerMessage msg) {
        StreamObserver<PeerMessage> s = sender;
        if (s == null) return;
        try {
            s.onNext(msg);
        } catch (Exception e) {
            sender = null;
        }
    }

    public void shutdown() {
        sender = null;
        channel.shutdownNow();
    }
}
