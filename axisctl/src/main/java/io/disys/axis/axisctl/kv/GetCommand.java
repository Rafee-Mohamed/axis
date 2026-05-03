package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.GetAtRequest;
import io.disys.axis.api.proto.GetRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "get",
        mixinStandardHelpOptions = true,
        description = "Get the value of a key. Use --at to read at a specific revision."
)
public class GetCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "KEY")
    String key;

    @Option(names = "--at", paramLabel = "REVISION",
            description = "Read the key as it existed at this store revision.",
            defaultValue = "0")
    long atRevision;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            if (atRevision > 0) {
                var req = GetAtRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setRevision(atRevision)
                        .build();
                var resp = client.kv().getAt(req);
                switch (resp.getResultCase()) {
                    case OK -> {
                        var ok = resp.getOk();
                        if (ok.getKv().getKey().isEmpty()) {
                            KvPrinter.printNotFoundAt(atRevision);
                        } else {
                            KvPrinter.printKvAt(ok.getKv(), atRevision);
                        }
                    }
                    case COMPACTED -> {
                        var c = resp.getCompacted();
                        System.out.printf("(compacted — rev:%d is gone, oldest available: %d)%n",
                                c.getRequestedRevision(), c.getFirstVisibleRevision());
                    }
                    case FUTURE -> {
                        var f = resp.getFuture();
                        System.out.printf("(future — rev:%d not yet committed, current: %d)%n",
                                f.getRequestedRevision(), f.getLastVisibleRevision());
                    }
                    default -> System.out.println("(unexpected response)");
                }
            } else {
                var req = GetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .build();
                var resp = client.kv().get(req);
                if (!resp.hasKv()) {
                    System.out.printf("rev:%d%n(not found)%n", resp.getHeader().getRevision());
                    return;
                }
                KvPrinter.printKv(resp.getKv(), resp.getHeader());
            }
        }
    }
}
