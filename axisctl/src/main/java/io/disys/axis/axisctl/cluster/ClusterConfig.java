package io.disys.axis.axisctl.cluster;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record ClusterConfig(String jar, long clusterId, List<NodeConfig> nodes) {

    public record NodeConfig(long id, String host, int clientPort, int peerPort, String dataDir) {
        public String peerAddress() {
            return host + ":" + peerPort;
        }
        public String clientAddress() {
            return host + ":" + clientPort;
        }
    }

    @SuppressWarnings("unchecked")
    public static ClusterConfig load(Path path) throws IOException {
        var yaml = new Yaml();
        Map<String, Object> root;
        try (var in = Files.newInputStream(path)) {
            root = yaml.load(in);
        }

        // resolve jar path relative to the yaml file's directory so cluster.yaml is portable
        var jarRaw = (String) root.get("jar");
        var jar = path.toAbsolutePath().getParent().resolve(jarRaw).normalize().toString();
        var clusterId = toLong(root.get("cluster-id"));

        var rawNodes = (List<Map<String, Object>>) root.get("nodes");
        var nodes = rawNodes.stream().map(n -> {
            var id = toLong(n.get("id"));
            var host = (String) n.getOrDefault("host", "localhost");
            var clientPort = toInt(n.get("client-port"));
            var peerPort = toInt(n.get("peer-port"));
            var dataDir = expandHome((String) n.get("data-dir"));
            return new NodeConfig(id, host, clientPort, peerPort, dataDir);
        }).toList();

        return new ClusterConfig(jar, clusterId, nodes);
    }

    /**
     * Loads the bundled default 3-node local cluster topology.
     * The server jar path is resolved relative to the axisctl jar's own location.
     */
    @SuppressWarnings("unchecked")
    public static ClusterConfig loadDefault() throws IOException {
        var yaml = new Yaml();
        Map<String, Object> root;
        try (var in = ClusterConfig.class.getResourceAsStream("/cluster-default.yaml")) {
            if (in == null) throw new IOException("bundled default-cluster.yaml not found in jar");
            root = yaml.load(in);
        }

        var jarRaw = (String) root.get("jar");
        var jar = resolveJarRelativeToSelf(jarRaw);
        var clusterId = toLong(root.get("cluster-id"));

        var rawNodes = (List<Map<String, Object>>) root.get("nodes");
        var nodes = rawNodes.stream().map(n -> {
            var id = toLong(n.get("id"));
            var host = (String) n.getOrDefault("host", "localhost");
            var clientPort = toInt(n.get("client-port"));
            var peerPort = toInt(n.get("peer-port"));
            var dataDir = expandHome((String) n.get("data-dir"));
            return new NodeConfig(id, host, clientPort, peerPort, dataDir);
        }).toList();

        return new ClusterConfig(jar, clusterId, nodes);
    }

    /**
     * Resolves a jar path relative to the directory containing the axisctl jar itself.
     * This lets the bundled default-cluster.yaml reference the server jar without any
     * absolute or user-specific paths.
     */
    private static String resolveJarRelativeToSelf(String relative) throws IOException {
        try {
            var selfUri = ClusterConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            var selfDir = Path.of(selfUri).getParent();
            return selfDir.resolve(relative).normalize().toString();
        } catch (URISyntaxException e) {
            throw new IOException("cannot determine axisctl jar location", e);
        }
    }

    private static String expandHome(String path) {
        if (path == null) return null;
        if (path.startsWith("~/")) return System.getProperty("user.home") + path.substring(1);
        if (path.equals("~")) return System.getProperty("user.home");
        return path;
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    private static int toInt(Object v) {
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }
}
