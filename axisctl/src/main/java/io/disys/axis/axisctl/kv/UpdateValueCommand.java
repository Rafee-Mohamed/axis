package io.disys.axis.axisctl.kv;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.UpdateValueRequest;
import io.disys.axis.api.proto.UpdateValueResponse;
import io.disys.axis.axisctl.AxisClient;
import io.disys.axis.axisctl.EndpointMixin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(
        name = "update-value",
        mixinStandardHelpOptions = true,
        description = "Update the value of an existing key, preserving its lease."
)
public class UpdateValueCommand implements Runnable {

    @Mixin EndpointMixin endpoint;

    @Parameters(index = "0", paramLabel = "KEY")
    String key;

    @Parameters(index = "1", paramLabel = "VALUE")
    String value;

    @Override
    public void run() {
        try (var client = new AxisClient(endpoint.host(), endpoint.port())) {
            var req = UpdateValueRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(key))
                    .setVal(ByteString.copyFromUtf8(value))
                    .build();
            var resp = client.kv().updateValue(req);
            switch (resp.getResultCase()) {
                case OK            -> KvPrinter.printUpdated(resp.getHeader());
                case KEY_NOT_FOUND -> KvPrinter.printNotFound(key, resp.getHeader());
                default            -> System.out.println("(unexpected response)");
            }
        }
    }
}
