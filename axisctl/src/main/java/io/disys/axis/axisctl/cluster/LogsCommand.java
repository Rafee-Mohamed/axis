package io.disys.axis.axisctl.cluster;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

@Command(
        name = "logs",
        mixinStandardHelpOptions = true,
        description = "Show logs for cluster nodes."
)
public class LogsCommand implements Runnable {

    @Parameters(index = "0", paramLabel = "TOPOLOGY", description = "Path to cluster.yaml (default: ./cluster.yaml)",
            defaultValue = "cluster.yaml")
    Path topology;

    @Option(names = {"-f", "--follow"}, description = "Stream log output continuously.")
    boolean follow;

    @Option(names = "--node", paramLabel = "NODE_ID", defaultValue = "0",
            description = "Show logs for a specific node only (default: all nodes).")
    long nodeId;

    @Option(names = "-n", paramLabel = "LINES", defaultValue = "50",
            description = "Number of recent lines to show per node when not following (default: 50).")
    int lines;

    @Override
    public void run() {
        try {
            var config = Files.exists(topology) ? ClusterConfig.load(topology) : ClusterConfig.loadDefault();
            var logPaths = config.nodes().stream()
                    .filter(n -> nodeId == 0 || n.id() == nodeId)
                    .map(n -> (Map.Entry<String, Path>) new AbstractMap.SimpleEntry<>(
                            "node-" + n.id(),
                            Path.of(n.dataDir()).resolve("server.log")))
                    .toList();

            if (follow) {
                followLogs(logPaths);
            } else {
                dumpLogs(logPaths);
            }
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }

    private void dumpLogs(List<Map.Entry<String, Path>> logPaths) throws IOException {
        for (var entry : logPaths) {
            var label = entry.getKey();
            var path = entry.getValue();
            if (!Files.exists(path)) {
                System.out.printf("[%s] (no log file yet)%n", label);
                continue;
            }
            var allLines = Files.readAllLines(path);
            var tail = allLines.subList(Math.max(0, allLines.size() - lines), allLines.size());
            for (var line : tail) {
                System.out.printf("[%s] %s%n", label, line);
            }
        }
    }

    private void followLogs(List<Map.Entry<String, Path>> logPaths) throws IOException, InterruptedException {
        var rafMap = logPaths.stream().collect(
                java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            try {
                                var path = e.getValue();
                                if (!Files.exists(path)) Files.createFile(path);
                                var raf = new RandomAccessFile(path.toFile(), "r");
                                raf.seek(raf.length());
                                return raf;
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        },
                        (a, b) -> a,
                        java.util.LinkedHashMap::new));

        System.out.println("streaming logs — Ctrl-C to stop");
        while (true) {
            for (var entry : rafMap.entrySet()) {
                String line;
                while ((line = entry.getValue().readLine()) != null) {
                    System.out.printf("[%s] %s%n", entry.getKey(), line);
                }
            }
            //noinspection BusyWait
            Thread.sleep(100);
        }
    }
}
