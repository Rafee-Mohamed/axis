package io.disys.axis.consensus.transport;

import io.disys.axis.consensus.proto.PeerMessage;
import io.disys.axis.consensus.proto.RaftTransportServiceGrpc;
import io.disys.axis.consensus.transport.codec.PeerMessageCodec;
import io.disys.jaft.message.Message;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public final class RaftTransportService extends RaftTransportServiceGrpc.RaftTransportServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RaftTransportService.class);

    private final Consumer<Message.Peer> receiver;

    public RaftTransportService(Consumer<Message.Peer> receiver) {
        this.receiver = receiver;
    }

    @Override
    public StreamObserver<PeerMessage> stream(StreamObserver<PeerMessage> responseObserver) {
        return new StreamObserver<>() {

            @Override
            public void onNext(PeerMessage msg) {
                receiver.accept(PeerMessageCodec.decode(msg));
            }

            @Override
            public void onError(Throwable t) {
                log.warn("Peer stream error", t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
