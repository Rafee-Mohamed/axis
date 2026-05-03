package io.disys.axis.axisctl.cluster;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;

@Command(
        name = "stop",
        mixinStandardHelpOptions = true,
        description = "Stop running cluster nodes. Use --node to stop a specific node."
)
public class StopCommand implements Runnable {

    @Parameters(index = "0", paramLabel = "TOPOLOGY", description = "Path to cluster.yaml (default: ./cluster.yaml)",
            defaultValue = "cluster.yaml")
    Path topology;

    @Option(names = "--node", paramLabel = "NODE_ID",
            description = "Stop only the node with this id (default: stop all nodes).",
            defaultValue = "0")
    long nodeId;

    @Override
    public void run() {
        try {
            var config = Files.exists(topology) ? ClusterConfig.load(topology) : ClusterConfig.loadDefault();
            new ClusterManager(config).stop(nodeId);
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }
}
