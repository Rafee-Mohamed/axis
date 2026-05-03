package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.RangeAtRequest;
import io.disys.axis.api.proto.RangeRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "range",
        mixinStandardHelpOptions = true,
        description = "List keys in the range [FROM, TO). Use --at to read at a specific revision."
)
public class RangeCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "FROM")
    String from;

    @Parameters(index = "1", paramLabel = "TO")
    String to;

    @Option(names = "--at", paramLabel = "REVISION",
            description = "Read the range as it existed at this store revision.",
            defaultValue = "0")
    long atRevision;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            if (atRevision > 0) {
                var req = RangeAtRequest.newBuilder()
                        .setFrom(ByteString.copyFromUtf8(from))
                        .setTo(ByteString.copyFromUtf8(to))
                        .setRevision(atRevision)
                        .build();
                var resp = client.kv().rangeAt(req);
                switch (resp.getResultCase()) {
                    case OK -> {
                        var ok = resp.getOk();
                        KvPrinter.printKvTableAt(ok.getKvsList(), atRevision, ok.getMore());
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
                var req = RangeRequest.newBuilder()
                        .setFrom(ByteString.copyFromUtf8(from))
                        .setTo(ByteString.copyFromUtf8(to))
                        .build();
                var resp = client.kv().range(req);
                KvPrinter.printKvTable(resp.getKvsList(), resp.getHeader(), resp.getMore());
            }
        }
    }
}
