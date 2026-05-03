package io.disys.axis.axisctl.lease;

import io.disys.axis.api.proto.RevokeRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "revoke",
        mixinStandardHelpOptions = true,
        description = "Revoke a lease and delete all keys attached to it."
)
public class RevokeCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "LEASE_ID", description = "Lease ID to revoke.")
    long leaseId;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = RevokeRequest.newBuilder().setLeaseId(leaseId).build();
            var resp = client.lease().revoke(req);
            switch (resp.getResultCase()) {
                case REVOKED  -> System.out.printf("lease %d revoked%n", leaseId);
                case NOT_FOUND -> System.out.printf("lease %d not found%n", leaseId);
                default        -> System.out.println("unknown response");
            }
        }
    }
}
