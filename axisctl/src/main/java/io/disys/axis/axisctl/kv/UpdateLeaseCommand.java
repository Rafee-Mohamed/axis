package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.UpdateLeaseRequest;
import io.disys.axis.api.proto.UpdateLeaseResponse;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "update-lease",
        mixinStandardHelpOptions = true,
        description = "Change the lease attached to an existing key."
)
public class UpdateLeaseCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "KEY")
    String key;

    @Parameters(index = "1", paramLabel = "LEASE_ID")
    long leaseId;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = UpdateLeaseRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(key))
                    .setLeaseId(leaseId)
                    .build();
            var resp = client.kv().updateLease(req);
            switch (resp.getResultCase()) {
                case OK            -> KvPrinter.printUpdated(resp.getHeader());
                case KEY_NOT_FOUND -> KvPrinter.printNotFound(key, resp.getHeader());
                case LEASE_NOT_FOUND -> System.out.printf("lease %d not found%n", resp.getLeaseNotFound().getLeaseId());
                default            -> System.out.println("(unexpected response)");
            }
        }
    }
}
