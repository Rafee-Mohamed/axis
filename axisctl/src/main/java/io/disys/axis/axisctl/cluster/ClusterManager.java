package io.disys.axis.axisctl.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class ClusterManager {

    private final ClusterConfig config;

    public ClusterManager(ClusterConfig config) {
        this.config = config;
    }

    public void start(long nodeId) throws IOException {
        var nodes = config.nodes().stream()
                .filter(n -> nodeId == 0 || n.id() == nodeId)
                .toList();

        if (nodes.isEmpty()) {
            System.out.printf("no node with id %d in config%n", nodeId);
            return;
        }

        var allVoters = config.nodes().stream()
                .map(n -> String.valueOf(n.id()))
                .collect(Collectors.joining(","));

        for (var node : nodes) {
            if (isRunning(node)) {
                System.out.printf("node %d already running (pid %d)%n", node.id(), readPid(node));
                continue;
            }

            var peers = config.nodes().stream()
                    .filter(n -> n.id() != node.id())
                    .map(n -> n.id() + "=" + n.peerAddress())
                    .collect(Collectors.joining(","));

            var cmd = buildCommand(node, allVoters, peers);
            var dataPath = Path.of(node.dataDir());
            var logFile = dataPath.resolve("server.log");
            Files.createDirectories(dataPath);

            var pb = new ProcessBuilder(cmd)
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));

            var proc = pb.start();
            writePid(node, proc.pid());

            System.out.printf("node %d started (pid %d) — log: %s%n", node.id(), proc.pid(), logFile);
        }
    }

    public void stop(long nodeId) throws IOException {
        var nodes = config.nodes().stream()
                .filter(n -> nodeId == 0 || n.id() == nodeId)
                .toList();

        for (var node : nodes) {
            var pidFile = pidFile(node);
            if (!Files.exists(pidFile)) {
                System.out.printf("node %d not running (no pid file)%n", node.id());
                continue;
            }
            var pid = readPid(node);
            var killed = ProcessHandle.of(pid).map(ph -> {
                ph.destroy();
                return true;
            }).orElse(false);

            Files.deleteIfExists(pidFile);
            if (killed) {
                System.out.printf("node %d stopped (pid %d)%n", node.id(), pid);
            } else {
                System.out.printf("node %d pid %d not found (already stopped?)%n", node.id(), pid);
            }
        }
    }

    public void status() {
        System.out.printf("%-6s  %-20s  %-10s  %s%n", "id", "endpoint", "status", "pid");
        for (var node : config.nodes()) {
            var pidFile = pidFile(node);
            String status;
            String pidStr;
            if (!Files.exists(pidFile)) {
                status = "stopped";
                pidStr = "-";
            } else {
                var pid = readPid(node);
                if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
                    status = "running";
                    pidStr = String.valueOf(pid);
                } else {
                    status = "dead";
                    pidStr = String.valueOf(pid);
                }
            }
            System.out.printf("%-6d  %-20s  %-10s  %s%n",
                    node.id(), node.clientAddress(), status, pidStr);
        }
    }

    private ArrayList<String> buildCommand(ClusterConfig.NodeConfig node, String allVoters, String peers) {
        var cmd = new ArrayList<String>();
        cmd.add("java");
        cmd.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        cmd.add("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");
        cmd.add("--enable-native-access=ALL-UNNAMED");
        cmd.add("--sun-misc-unsafe-memory-access=allow");
        cmd.add("-jar");
        cmd.add(config.jar());
        cmd.add("--member-id=" + node.id());
        cmd.add("--cluster-id=" + config.clusterId());
        cmd.add("--initial-voters=" + allVoters);
        cmd.add("--client-port=" + node.clientPort());
        cmd.add("--peer-port=" + node.peerPort());
        cmd.add("--data-dir=" + node.dataDir());
        if (!peers.isEmpty()) {
            cmd.add("--peers=" + peers);
        }
        return cmd;
    }

    private boolean isRunning(ClusterConfig.NodeConfig node) {
        var pidFile = pidFile(node);
        if (!Files.exists(pidFile)) return false;
        var pid = readPid(node);
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static Path pidFile(ClusterConfig.NodeConfig node) {
        return Path.of(node.dataDir()).resolve("axis.pid");
    }

    private static long readPid(ClusterConfig.NodeConfig node) {
        try {
            return Long.parseLong(Files.readString(pidFile(node)).strip());
        } catch (IOException e) {
            return -1;
        }
    }

    private static void writePid(ClusterConfig.NodeConfig node, long pid) throws IOException {
        Files.writeString(pidFile(node), String.valueOf(pid));
    }
}
