package io.disys.axis.axisctl.lease;

import io.disys.axis.api.proto.InfoRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "info",
        mixinStandardHelpOptions = true,
        description = "Show details for a lease including attached keys."
)
public class InfoCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "LEASE_ID")
    long leaseId;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = InfoRequest.newBuilder().setLeaseId(leaseId).build();
            var resp = client.lease().info(req);
            switch (resp.getResultCase()) {
                case FOUND -> LeasePrinter.printLeaseInfo(resp.getFound());
                case NOT_FOUND -> System.out.printf("lease %d not found%n", leaseId);
                default        -> System.out.println("unknown response");
            }
        }
    }
}
