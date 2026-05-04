# Configuration

Axis has two separate configuration surfaces: the **cluster topology** used by axisctl to
manage node processes, and the **server config** that controls the behaviour of each node
at runtime.

---

## Cluster topology

The cluster topology file tells axisctl which nodes exist, where they listen, and where
their data lives. It is used by `cluster start`, `stop`, `status`, and `logs`.

A bundled default topology is built into the axisctl jar and used automatically when no
file is provided. To customise it, create a YAML file and pass it to any cluster command:

```bash
axisctl cluster start my-cluster.yaml
```

### Format

```yaml
jar: ../server/target/server-0.1.0-SNAPSHOT.jar
cluster-id: 1
nodes:
  - id: 1
    client-port: 2379
    peer-port: 2380
    data-dir: ~/.axis/node-1
  - id: 2
    client-port: 2479
    peer-port: 2480
    data-dir: ~/.axis/node-2
  - id: 3
    client-port: 2579
    peer-port: 2580
    data-dir: ~/.axis/node-3
```

### Fields

| Field              | Required | Description                                                  |
|--------------------|----------|--------------------------------------------------------------|
| `jar`              | yes      | Path to the server fat jar. Resolved relative to the YAML file's directory. |
| `cluster-id`       | yes      | Numeric ID shared by all nodes in the cluster.               |
| `nodes[].id`       | yes      | Unique numeric ID for this node.                             |
| `nodes[].client-port` | yes   | Port for client gRPC connections.                            |
| `nodes[].peer-port`   | yes   | Port for Raft peer-to-peer communication.                    |
| `nodes[].data-dir` | yes      | Directory for this node's data. `~` is expanded to the home directory. |
| `nodes[].host`     | no       | Hostname or IP. Defaults to `localhost`.                     |

`axisctl cluster start` derives `--initial-voters` and `--peers` from the node list
automatically - you do not need to set these manually.

---

## Server config

Each node reads a YAML config file at startup that controls storage sizing, MVCC behaviour,
lease management, and Raft timing. The server jar includes a built-in default config that
is used when no `--config` flag is passed.

To override specific settings, create a YAML file with only the fields you want to change
and pass it with `--config`:

```bash
java -jar server.jar --member-id=1 ... --config=/path/to/axis.yaml
```

Any field omitted from the file falls back to the default value shown below.

### Server startup flags

These are passed on the command line and are required for every node:

| Flag                | Default | Description                                                  |
|---------------------|---------|--------------------------------------------------------------|
| `--member-id`       | -       | Unique numeric ID for this node within the cluster.          |
| `--cluster-id`      | -       | Numeric ID of the cluster. Must match across all nodes.      |
| `--data-dir`        | -       | Root directory for this node's data (lmdb/, wal/).           |
| `--initial-voters`  | -       | Comma-separated node IDs forming the initial voter set. e.g. `1,2,3` |
| `--client-port`     | `2379`  | Port for client gRPC connections.                            |
| `--peer-port`       | `2380`  | Port for Raft peer communication.                            |
| `--peers`           | -       | Addresses of peer nodes. e.g. `2=localhost:2480,3=localhost:2580` |
| `--config`          | -       | Path to an axis config YAML file. Uses the bundled default if omitted. |

### LMDB

Controls the embedded storage backend.

```yaml
lmdb:
  map-size: 10737418240   # 10 GB
```

| Field      | Default | Description                                                       |
|------------|---------|-------------------------------------------------------------------|
| `map-size` | 10 GB   | Maximum size of the LMDB memory-mapped file in bytes. The file is sparse on most filesystems - disk usage grows with actual data, not this limit. Increase if writes fail with a map-full error. |

### WAL

Controls the write-ahead log used to persist Raft entries.

```yaml
wal:
  segment-size: 67108864   # 64 MB
```

| Field          | Default | Description                                                   |
|----------------|---------|---------------------------------------------------------------|
| `segment-size` | 64 MB   | Size of each WAL segment file in bytes. Older segments are retained until Raft log compaction (snapshots) is implemented. |

### Raft

Raft timers are expressed in **ticks**. The real duration of each timeout is
`tick-count x tick-interval`.

```yaml
tick-interval: "PT0.01S"   # 10ms per tick

raft:
  election-timeout: 100    # 100 ticks = 1000ms base, randomised 1000ms-1900ms
  heartbeat-timeout: 10    # 10 ticks  = 100ms
  max-inflight-msgs: 256
  max-applying-entries-size: 67108864   # 64 MB
```

| Field                       | Default        | Description                                     |
|-----------------------------|----------------|-------------------------------------------------|
| `tick-interval`             | 10ms           | Base clock period. All Raft timeouts multiply against this. ISO-8601 duration string. |
| `raft.election-timeout`     | 100 ticks (1s) | How long a follower waits without a heartbeat before starting an election. Actual timeout is randomised between 1x and 2x this value to avoid split votes. Must be at least 5x `heartbeat-timeout`. |
| `raft.heartbeat-timeout`    | 10 ticks (100ms) | How often the leader sends heartbeats to followers. |
| `raft.max-inflight-msgs`    | 256            | Maximum number of in-flight replication messages per follower. |
| `raft.max-applying-entries-size` | 64 MB     | Maximum total size of log entries being applied to the state machine at once. |

### Node

Controls shutdown behaviour.

```yaml
node:
  shutdown-timeout: "PT10S"
  work-termination-timeout: "PT5S"
```

| Field                      | Default | Description                                              |
|----------------------------|---------|----------------------------------------------------------|
| `shutdown-timeout`         | 10s     | How long to wait for the node to shut down cleanly before forcing termination. |
| `work-termination-timeout` | 5s      | How long to wait for in-progress work to finish during shutdown. |

### MVCC

Controls the versioned store's write buffer and compaction.

```yaml
mvcc:
  max-buffer: 1024
  buffer-sync-timeout: "PT5S"
  delete-batch-size: 256
  index-max-keys: 64
```

| Field                 | Default | Description                                                    |
|-----------------------|---------|----------------------------------------------------------------|
| `max-buffer`          | 1024    | Maximum number of revision records held in the write buffer before a backend sync is forced. |
| `buffer-sync-timeout` | 5s      | Maximum time a write session can stay open before its buffer is flushed to the backend. |
| `delete-batch-size`   | 256     | Number of obsolete revision records removed from the backend per compaction batch. Larger values compact faster but add more write latency per batch. |
| `index-max-keys`      | 64      | Maximum number of keys per B+ tree node in the in-memory timeline index. |

### Lease

Controls lease TTL enforcement and checkpoint replication.

```yaml
lease:
  min-ttl: 1
  revoke-rate: 1000
  checkpoint-batch-size: 1000
  checkpoint-batch-rate: 1000
  checkpoint-interval: "PT5M"
  min-wait-time: "PT0.5S"
  extend-on-schedule-track: "PT2S"
```

| Field                     | Default | Description                                                   |
|---------------------------|---------|---------------------------------------------------------------|
| `min-ttl`                 | 1s      | Minimum TTL in seconds that can be granted.                   |
| `revoke-rate`             | 1000    | Maximum number of lease revocations processed per second.     |
| `checkpoint-batch-size`   | 1000    | Number of leases included in a single checkpoint batch.       |
| `checkpoint-batch-rate`   | 1000    | Maximum number of checkpoints written per second.             |
| `checkpoint-interval`     | 5m      | How often the leader checkpoints remaining TTLs to followers so they can take over expiry enforcement after a leadership change. |
| `min-wait-time`           | 500ms   | Minimum interval between scheduler wake-ups. Prevents busy-looping when many leases expire at the same time. |
| `extend-on-schedule-track`| 2s      | Grace period added to lease deadlines when the leader starts tracking schedules, giving followers time to sync before the first expiry fires. |
