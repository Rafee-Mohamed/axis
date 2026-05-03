package io.disys.axis.axisctl.cluster;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;

@Command(
        name = "status",
        mixinStandardHelpOptions = true,
        description = "Show the running status of all nodes in the cluster topology."
)
public class StatusCommand implements Runnable {

    @Parameters(index = "0", paramLabel = "TOPOLOGY", description = "Path to cluster.yaml (default: ./cluster.yaml)",
            defaultValue = "cluster.yaml")
    Path topology;

    @Override
    public void run() {
        try {
            var config = Files.exists(topology) ? ClusterConfig.load(topology) : ClusterConfig.loadDefault();
            new ClusterManager(config).status();
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }
}
