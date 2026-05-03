package io.disys.axis.axisctl.lease;

import io.disys.axis.api.proto.LeasesRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(
        name = "list",
        mixinStandardHelpOptions = true,
        description = "List all active leases."
)
public class ListCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var resp = client.lease().leases(LeasesRequest.getDefaultInstance());
            LeasePrinter.printLeaseTable(resp.getLeasesList());
        }
    }
}
