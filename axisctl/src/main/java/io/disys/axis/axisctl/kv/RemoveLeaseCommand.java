package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.RemoveLeaseRequest;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "remove-lease",
        mixinStandardHelpOptions = true,
        description = "Detach the lease from a key, keeping the key without a lease."
)
public class RemoveLeaseCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "KEY")
    String key;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = RemoveLeaseRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(key))
                    .build();
            var resp = client.kv().removeLease(req);
            KvPrinter.printUpdated(resp.getHeader());
        }
    }
}
