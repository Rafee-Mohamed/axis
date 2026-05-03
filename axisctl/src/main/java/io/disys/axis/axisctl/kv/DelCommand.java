package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.DeleteAndGetRequest;
import io.disys.axis.api.proto.DeleteRangeAndGetRequest;
import io.disys.axis.api.proto.DeleteRangeRequest;
import io.disys.axis.api.proto.DeleteRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "del",
        mixinStandardHelpOptions = true,
        description = "Delete a key or range [FROM, TO). Use --prev to return deleted value(s)."
)
public class DelCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "FROM")
    String from;

    @Parameters(index = "1", paramLabel = "TO", arity = "0..1",
            description = "End of range (exclusive). If omitted, deletes a single key.")
    String to;

    @Option(names = "--prev",
            description = "Return the value(s) before deletion.")
    boolean prev;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            if (to != null) {
                rangeDelete(client);
            } else {
                singleDelete(client);
            }
        }
    }

    private void singleDelete(AxisClient client) {
        var k = ByteString.copyFromUtf8(from);
        if (prev) {
            var req = DeleteAndGetRequest.newBuilder().setKey(k).build();
            var resp = client.kv().deleteAndGet(req);
            if (resp.hasPrevKv() && !resp.getPrevKv().getKey().isEmpty()) {
                KvPrinter.printDeleted(from, resp.getHeader());
                KvPrinter.printPrev(resp.getPrevKv());
            } else {
                KvPrinter.printNotFound(from, resp.getHeader());
            }
        } else {
            var req = DeleteRequest.newBuilder().setKey(k).build();
            var resp = client.kv().delete(req);
            if (resp.getDeleted()) {
                KvPrinter.printDeleted(from, resp.getHeader());
            } else {
                KvPrinter.printNotFound(from, resp.getHeader());
            }
        }
    }

    private void rangeDelete(AxisClient client) {
        var f = ByteString.copyFromUtf8(from);
        var t = ByteString.copyFromUtf8(to);
        if (prev) {
            var req = DeleteRangeAndGetRequest.newBuilder().setFrom(f).setTo(t).build();
            var resp = client.kv().deleteRangeAndGet(req);
            System.out.printf("deleted  %d  [rev:%d]%n",
                    resp.getPrevKvsList().size(), resp.getHeader().getRevision());
            if (!resp.getPrevKvsList().isEmpty()) {
                KvPrinter.printPrevMany(resp.getPrevKvsList());
            }
        } else {
            var req = DeleteRangeRequest.newBuilder().setFrom(f).setTo(t).build();
            var resp = client.kv().deleteRange(req);
            System.out.printf("deleted  %d  [rev:%d]%n",
                    resp.getDeleted(), resp.getHeader().getRevision());
        }
    }
}
