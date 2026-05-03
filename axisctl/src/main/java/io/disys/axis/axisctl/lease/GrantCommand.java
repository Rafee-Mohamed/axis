package io.disys.axis.axisctl.lease;

import io.disys.axis.api.proto.GrantRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "grant",
        mixinStandardHelpOptions = true,
        description = "Create a new lease with the given TTL in seconds."
)
public class GrantCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "TTL", description = "Lease TTL in seconds.")
    long ttl;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = GrantRequest.newBuilder().setTtl(ttl).build();
            var resp = client.lease().grant(req);
            System.out.printf("lease %d granted with TTL(%ds)%n", resp.getLeaseId(), resp.getTtl());
        }
    }
}
