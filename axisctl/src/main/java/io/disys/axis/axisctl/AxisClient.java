package io.disys.axis.axisctl;

import io.disys.axis.api.proto.KvServiceGrpc;
import io.disys.axis.api.proto.LeaseServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

public class AxisClient implements AutoCloseable {

    private final ManagedChannel channel;

    public AxisClient(String host, int port) {
        channel = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public KvServiceGrpc.KvServiceBlockingStub kv() {
        return KvServiceGrpc.newBlockingStub(channel);
    }

    public LeaseServiceGrpc.LeaseServiceBlockingStub lease() {
        return LeaseServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }
}
