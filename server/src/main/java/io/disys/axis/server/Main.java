package io.disys.axis.server;

import io.disys.axis.backend.Database;
import io.disys.axis.backend.lmdb.LmdbConfig;
import io.disys.axis.wal.api.WalConfig;
import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.core.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    static void main(String[] args) throws Exception {
        var flags = parseFlags(args);

        long memberId   = requireLong(flags, "member-id");
        long clusterId  = requireLong(flags, "cluster-id");
        var  dataDir    = Path.of(require(flags, "data-dir"));
        var  voters     = parseVoters(require(flags, "initial-voters"));
        var  peers      = parsePeers(flags.getOrDefault("peers", ""));

        int clientPort = intFlag(flags, "client-port", 2379);
        int peerPort   = intFlag(flags, "peer-port", 2380);

        var configPath = flags.get("config");
        var axisConfig = configPath != null
                ? ConfigLoader.load(Path.of(configPath))
                : ConfigLoader.loadDefault();

        var lmdb = LmdbConfig.builder()
                .directory(dataDir.resolve("lmdb"))
                .databases(Set.of(
                        Database.of(axisConfig.mvcc().versionDB()),
                        Database.of(axisConfig.mvcc().metaDB()),
                        Database.of(axisConfig.lease().leaseDb())
                ))
                .snapshotDestination(dataDir.resolve("snapshot"))
                .mapSize(axisConfig.lmdbMapSize())
                .build();

        var wal = new WalConfig.Builder()
                .directory(dataDir.resolve("wal"))
                .segmentSize(axisConfig.walSegmentSize())
                .build();

        var config = ServerConfig.builder()
                .clusterId(clusterId)
                .memberId(memberId)
                .clientPort(clientPort)
                .peerPort(peerPort)
                .peers(peers)
                .initialMembership(MembershipConfig.of(voters, Set.of()))
                .tickInterval(axisConfig.tickInterval())
                .lmdb(lmdb)
                .wal(wal)
                .mvcc(axisConfig.mvcc())
                .lease(axisConfig.lease())
                .raft(axisConfig.raft())
                .node(axisConfig.node())
                .build();

        var server = AxisServer.create(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            try {
                server.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        server.start();
        server.awaitTermination();
    }

    private static Map<String, String> parseFlags(String[] args) {
        var flags = new HashMap<String, String>();
        int i = 0;
        while (i < args.length) {
            var arg = args[i];
            if (arg.startsWith("--")) {
                var key = arg.substring(2);
                var eq = key.indexOf('=');
                if (eq >= 0) {
                    flags.put(key.substring(0, eq), key.substring(eq + 1));
                } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    flags.put(key, args[++i]);
                } else {
                    flags.put(key, "true");
                }
            }
            i++;
        }
        return flags;
    }

    private static String require(Map<String, String> flags, String key) {
        var v = flags.get(key);
        if (v == null) throw new IllegalArgumentException("missing required flag: --" + key);
        return v;
    }

    private static long requireLong(Map<String, String> flags, String key) {
        return Long.parseLong(require(flags, key));
    }

    private static int intFlag(Map<String, String> flags, String key, int def) {
        return flags.containsKey(key) ? Integer.parseInt(flags.get(key)) : def;
    }

    private static Set<NodeId> parseVoters(String csv) {
        return Stream.of(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> new NodeId(Long.parseLong(s)))
                .collect(Collectors.toSet());
    }

    // format: "2=host:port,3=host:port"
    private static Map<NodeId, InetSocketAddress> parsePeers(String csv) {
        if (csv.isBlank()) return Map.of();
        return Stream.of(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toMap(
                        s -> new NodeId(Long.parseLong(s.substring(0, s.indexOf('=')))),
                        s -> {
                            var addr = s.substring(s.indexOf('=') + 1);
                            var colon = addr.lastIndexOf(':');
                            return new InetSocketAddress(addr.substring(0, colon),
                                    Integer.parseInt(addr.substring(colon + 1)));
                        }
                ));
    }
}
