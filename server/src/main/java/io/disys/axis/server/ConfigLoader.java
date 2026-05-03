package io.disys.axis.server;

import io.disys.axis.lease.store.LeaseStoreConfig;
import io.disys.axis.mvcc.store.TimelineVersionedStoreConfig;
import io.disys.jaft.config.RaftConfig;
import io.disys.jaft.engine.ExecutionModel;
import io.disys.jaft.node.NodeConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

final class ConfigLoader {

    record AxisConfig(
            long lmdbMapSize,
            long walSegmentSize,
            TimelineVersionedStoreConfig mvcc,
            LeaseStoreConfig lease,
            RaftConfig raft,
            NodeConfig node,
            Duration tickInterval
    ) {}

    static AxisConfig loadDefault() throws IOException {
        try (var in = ConfigLoader.class.getResourceAsStream("/axis-default.yaml")) {
            if (in == null) throw new IOException("bundled default config not found");
            return parse(in);
        }
    }

    static AxisConfig load(Path file) throws IOException {
        try (var in = Files.newInputStream(file)) {
            return parse(in);
        }
    }

    @SuppressWarnings("unchecked")
    private static AxisConfig parse(InputStream in) {
        Map<String, Object> root = (Map<String, Object>) new Yaml()
                .load(new InputStreamReader(in, StandardCharsets.UTF_8));
        if (root == null) root = Map.of();

        var lmdb  = section(root, "lmdb");
        var wal   = section(root, "wal");
        var mvcc  = section(root, "mvcc");
        var lease = section(root, "lease");
        var raft  = section(root, "raft");
        var node  = section(root, "node");

        return new AxisConfig(
                longVal(lmdb, "map-size",     10L * 1024 * 1024 * 1024),
                longVal(wal,  "segment-size", 64L * 1024 * 1024),
                buildMvcc(mvcc),
                buildLease(lease),
                buildRaft(raft),
                buildNode(node),
                duration(root, "tick-interval", Duration.ofMillis(100))
        );
    }

    private static TimelineVersionedStoreConfig buildMvcc(Map<String, Object> m) {
        return new TimelineVersionedStoreConfig(
                intVal(m,  "max-buffer",           1024),
                duration(m, "buffer-sync-timeout", Duration.ofSeconds(5)),
                intVal(m,  "delete-batch-size",    256),
                intVal(m,  "index-max-keys",        64),
                strVal(m,  "version-db",           "axis_version"),
                strVal(m,  "meta-db",              "axis_meta"),
                strVal(m,  "persisted-commit-seq-key",          "persisted_commit_seq"),
                strVal(m,  "first-commit-seq-key",              "first_commit_seq"),
                strVal(m,  "compacted-revision-key",            "compacted_revision"),
                strVal(m,  "completed-compaction-commit-seq-key", "completed_compaction_commit_seq")
        );
    }

    private static LeaseStoreConfig buildLease(Map<String, Object> m) {
        return new LeaseStoreConfig(
                longVal(m, "min-ttl",                  1),
                intVal(m,  "revoke-rate",           1000),
                intVal(m,  "checkpoint-batch-size", 1000),
                intVal(m,  "checkpoint-batch-rate", 1000),
                duration(m, "checkpoint-interval",  Duration.ofMinutes(5)),
                duration(m, "min-wait-time",         Duration.ofMillis(500)),
                duration(m, "extend-on-schedule-track", Duration.ofSeconds(2)),
                strVal(m,  "lease-db",             "axis_leases")
        );
    }

    private static RaftConfig buildRaft(Map<String, Object> m) {
        return RaftConfig.builder()
                .electionTimeout(intVal(m,  "election-timeout",         10))
                .heartbeatTimeout(intVal(m, "heartbeat-timeout",          3))
                .maxInflightMsgs(intVal(m,  "max-inflight-msgs",        256))
                .maxApplyingEntriesSize(longVal(m, "max-applying-entries-size", 64L * 1024 * 1024))
                .executionModel(ExecutionModel.SEQUENTIAL)
                .build();
    }

    private static NodeConfig buildNode(Map<String, Object> m) {
        return new NodeConfig(
                duration(m, "shutdown-timeout",          Duration.ofSeconds(10)),
                duration(m, "work-termination-timeout",  Duration.ofSeconds(5))
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> map, String key) {
        var v = map.get(key);
        return v instanceof Map<?,?> ? (Map<String, Object>) v : Map.of();
    }

    private static int intVal(Map<String, Object> map, String key, int def) {
        var v = map.get(key);
        return v instanceof Number n ? n.intValue() : def;
    }

    private static long longVal(Map<String, Object> map, String key, long def) {
        var v = map.get(key);
        return v instanceof Number n ? n.longValue() : def;
    }

    private static String strVal(Map<String, Object> map, String key, String def) {
        var v = map.get(key);
        return v instanceof String s ? s : def;
    }

    private static Duration duration(Map<String, Object> map, String key, Duration def) {
        var v = map.get(key);
        if (v instanceof String s) return Duration.parse(s);
        if (v instanceof Number n) return Duration.ofMillis(n.longValue());
        return def;
    }
}
