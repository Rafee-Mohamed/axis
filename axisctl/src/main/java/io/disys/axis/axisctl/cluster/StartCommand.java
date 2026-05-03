package io.disys.axis.axisctl.cluster;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;

@Command(
        name = "start",
        mixinStandardHelpOptions = true,
        description = "Start all nodes defined in the cluster topology file. " +
                      "Use --node to start a specific node only."
)
public class StartCommand implements Runnable {

    @Parameters(index = "0", paramLabel = "TOPOLOGY", description = "Path to cluster.yaml (default: ./cluster.yaml)",
            defaultValue = "cluster.yaml")
    Path topology;

    @Option(names = "--node", paramLabel = "NODE_ID",
            description = "Start only the node with this id (default: start all nodes).",
            defaultValue = "0")
    long nodeId;

    @Override
    public void run() {
        try {
            var config = Files.exists(topology) ? ClusterConfig.load(topology) : ClusterConfig.loadDefault();
            new ClusterManager(config).start(nodeId);
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }
}
