package io.disys.axis.axisctl;

import picocli.CommandLine.Option;

public class EndpointMixin {

    @Option(
            names = {"--endpoint", "-e"},
            description = "Server endpoint in host:port form (default: localhost:2379)",
            defaultValue = "localhost:2379",
            paramLabel = "HOST:PORT"
    )
    public String endpoint;

    public String host() {
        int colon = endpoint.lastIndexOf(':');
        if (colon < 0) return endpoint;
        return endpoint.substring(0, colon);
    }

    public int port() {
        int colon = endpoint.lastIndexOf(':');
        if (colon < 0) return 2379;
        try {
            return Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port in endpoint: " + endpoint);
        }
    }
}
