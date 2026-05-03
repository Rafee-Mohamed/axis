package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.*;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "put",
        mixinStandardHelpOptions = true,
        description = "Set a key to a value. Use --lease to attach to a lease, --prev to return the old value."
)
public class PutCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "KEY")
    String key;

    @Parameters(index = "1", paramLabel = "VALUE")
    String value;

    @Option(names = "--lease", paramLabel = "LEASE_ID",
            description = "Attach the key to this lease. Key is deleted when the lease expires or is revoked.",
            defaultValue = "0")
    long leaseId;

    @Option(names = "--prev",
            description = "Return the previous value of the key before this put.")
    boolean prev;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var k = ByteString.copyFromUtf8(key);
            var v = ByteString.copyFromUtf8(value);

            if (prev) {
                if (leaseId != 0) {
                    var req = PutWithLeaseAndGetRequest.newBuilder()
                            .setKey(k).setVal(v).setLeaseId(leaseId).build();
                    var resp = client.kv().putWithLeaseAndGet(req);
                    switch (resp.getResultCase()) {
                        case OK -> {
                            KvPrinter.printStored(key, value, resp.getHeader(), leaseId);
                            var ok = resp.getOk();
                            if (ok.hasPrevKv() && !ok.getPrevKv().getKey().isEmpty()) {
                                KvPrinter.printPrev(ok.getPrevKv());
                            } else {
                                KvPrinter.printPrevAbsent();
                            }
                        }
                        case NOT_FOUND -> System.out.printf("lease %d not found%n", resp.getNotFound().getLeaseId());
                        default -> System.out.println("(unexpected response)");
                    }
                } else {
                    var req = PutAndGetRequest.newBuilder().setKey(k).setVal(v).build();
                    var resp = client.kv().putAndGet(req);
                    KvPrinter.printStored(key, value, resp.getHeader(), 0);
                    if (resp.hasPrevKv() && !resp.getPrevKv().getKey().isEmpty()) {
                        KvPrinter.printPrev(resp.getPrevKv());
                    } else {
                        KvPrinter.printPrevAbsent();
                    }
                }
            } else {
                if (leaseId != 0) {
                    var req = PutWithLeaseRequest.newBuilder()
                            .setKey(k).setVal(v).setLeaseId(leaseId).build();
                    var resp = client.kv().putWithLease(req);
                    switch (resp.getResultCase()) {
                        case OK -> KvPrinter.printStored(key, value, resp.getHeader(), leaseId);
                        case NOT_FOUND -> System.out.printf("lease %d not found%n", resp.getNotFound().getLeaseId());
                        default -> System.out.println("(unexpected response)");
                    }
                } else {
                    var req = PutRequest.newBuilder().setKey(k).setVal(v).build();
                    var resp = client.kv().put(req);
                    KvPrinter.printStored(key, value, resp.getHeader(), 0);
                }
            }
        }
    }
}
