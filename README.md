# Axis

Axis is a fault-tolerant, strongly consistent distributed key-value store designed for
coordination, distributed locking, and metadata management.

Inspired by [etcd](https://github.com/etcd-io/etcd). Axis implements the same core data
model - versioned keys, TTL leases, linearizable operations, and transactions - built from
first principles in Java.


## Features

- **Replicated log and total ordering** - all writes are serialized through a Raft
  consensus log. Every mutation has a unique, immutable position in a total order agreed
  upon by the cluster. No two writes can interleave.

- **Linearizable reads and writes** - every read reflects all writes that completed before
  it. Clients always observe a consistent, up-to-date view of the store regardless of which
  node they connect to.

- **Multi-version history** - every write creates a new revision. The full history of every
  key is retained. Nothing is overwritten in place.

- **Historical reads** - read any key or range as it existed at any past revision. Query
  what the store looked like at any point in its history.

- **TTL leases** - keys attach to leases. When a lease expires or is revoked, all attached
  keys are deleted automatically as a single atomic operation. The foundation for session
  management, distributed locks, and ephemeral registrations.

- **Range scans** - scan key ranges with filtering by revision window and sorting by key,
  value, version, or revision.

- **Transactions** - atomic operations with condition evaluation and success/failure
  branches. Conditions compare version, revision, value, or lease ID. All read and write
  operations are supported inside transactions, including nested transactions.


## Architecture

Axis runs as a cluster of replicated nodes. All requests - reads and writes - enter
through the gRPC layer of any node, pass through the consensus layer where writes are
serialized and replicated, and are applied to the storage layer.

```
   CLIENTS                LEADER (node 1)

  +----------+       +---------------------------------+
  | client 1 | gRPC  |            KvService            |
  | client 2 +-----> |           LeaseService          |
  | client 3 |  W/R  +-----------------+---------------+
  +----------+                         |
                                       v
                      +----------------+----------+                   +---------------------+
                      |  SequentialExecutor       +--- replicate ---> | node 2  (follower)  |
                      |  W: propose to Raft       |<-- quorum ack --  | Raft log + WAL      |
                      |  R: quorum confirmation   |                   | StoreWriter         |
                      |                           |                   | MVCC + Lease Store  |
                      |                           |                   +---------------------+
                      |  +--------+  +----------+ +--- replicate ---> | node 3  (follower)  |
                      |  |  Raft  |  |   WAL    | |<-- quorum ack --  | Raft log + WAL      |
                      |  +--------+  +----------+ |                   | StoreWriter         |
                      +-------------+-------------+                   | MVCC + Lease Store  |
                                    |                                 +---------------------+
                          W: after quorum commit
                        R: after quorum confirmation
                                    |
                      +-------------v-------------+
                      |  StoreWriter  (writes)    |
                      |  StoreReader  (reads)     |
                      +-------------+-------------+
                                    |
                      +-------------v-------------+
                      |        MVCC Store         |
                      |        Lease Store        |
                      +---------------------------+
```

Writes are proposed to Raft, written to the WAL, replicated to a quorum of followers,
and applied to the state machine via `StoreWriter`. Reads are served after a quorum
confirmation to ensure they reflect all prior committed writes, then served directly
from the store via `StoreReader`.

### MVCC

The MVCC store is the versioned key-value layer. Every write advances a global,
monotonically increasing revision - the logical clock of the store. Each revision
represents a unique position in the total order of all mutations across all keys.

Each key has a history on this timeline: a sequence of revisions showing when it was
created, updated, deleted, and possibly recreated. No revision is ever overwritten.
Any past state is queryable by specifying a revision.

```
+----------------------------------------------------------------------------------------+
|                                                                                        |
|  key a    ●---------------------●----------○                     ●-------------------> |
|                                                                                        |
|  key b               ●--------------------------------●----------○                     |
|                                                                                        |
|  key c                          ●----------------------------------------------------> |
|                                                                                        |
|-----------+----------+----------+----------+----------+----------+-------------------> |
|           2          4          6          8         10         12         revision    |
+----------------------------------------------------------------------------------------+

  ●  value written (live)     ○  key deleted (tombstone)     ->  key still alive
```

A read at revision 7 for `key a` returns the value written at revision 6. A read at
revision 9 returns not found. The current value is always the most recent revision.

The store is a single-writer, multiple-reader system. The writer advances the revision;
each reader is pinned to a snapshot at the revision it started and is unaffected by
concurrent or subsequent writes. Reads and writes never block each other.

See [mvcc/README.md](mvcc/README.md) for a detailed breakdown of the storage model.

### Write-ahead log

The WAL is an append-only, segmented log that makes Raft entries durable before they
are applied to the state machine.

```
  record                              record
+--------+---------------+--------+  +--------+---------------+--------+
| length |     data      | CRC32  |->| length |     data      | CRC32  |->  ...
+--------+---------------+--------+  +--------+---------------+--------+


  segment 0          segment 1        active segment
+---------------+  +---------------+  +---------------+
| record        |  | record        |  | record        |
| record        |  | record        |  | ...           |
| ...           |  | ...           |  |               |
+---------------+  +---------------+  +---------------+
    (sealed)           (sealed)          (appending)
```

Each record carries a CRC32 (cyclic redundancy check) checksum. Checksums chain from
one record to the next - each CRC covers both the record data and the previous record's
checksum. Any truncation, gap, or silent corruption breaks the chain and is detectable
on recovery even if the surrounding bytes and file offsets appear intact. A partial
write from a crash cannot silently pass as a valid record.

### Consensus

The `SequentialExecutor` is the entry point for all reads and writes. It drives Raft
through a process-advance loop, proposes commands as log entries, and applies committed
entries to the state machine (`StoreWriter`) in the order Raft commits them.

Leadership is managed automatically. If the current leader fails, a new election runs
and a new leader is elected without operator intervention. Writes proposed to a
non-leader are forwarded to the current leader.

### Lease store

Each lease is a timer object: a numeric ID, a TTL granted at creation, and a set of
keys currently attached to it.

Only the cluster leader schedules and enforces expiry. When a lease's TTL runs out, the
leader issues a replicated `LeaseRevoke` command through Raft. All nodes apply it
identically - the attached keys are deleted and the lease metadata removed in one
atomic state machine step. Expiry is never a silent local delete on followers.

To survive leader failover without losing how much time a lease had left, the leader
periodically replicates a `LeaseCheckpoint` entry carrying the remaining TTL of each
active lease. A new leader that takes over uses the last checkpointed value as its
starting point, rather than resetting to the original grant TTL.

Lease state - which keys are attached to which lease - is stored in and recovered
from the MVCC store.

### Backend

The backend is the storage abstraction layer that sits beneath the MVCC store and the
Lease store. It defines a generic interface - `Database`, `WriteHandle`, read and write
transactions - that the upper layers talk to without knowing the underlying storage
engine.

The only implementation is `backend-lmdb`: an embedded, memory-mapped storage engine
with native support for concurrent readers and a single writer. LMDB's page-level MVCC
means readers never block writers and writers never block readers, which aligns directly
with the SWMR model the MVCC store presents.


## Module structure

```
axis/
+-- api/          - protobuf definitions and generated gRPC stubs
|                   KvService, LeaseService, WatchService
|
+-- backend/      - storage backend abstraction
|                   Database, WriteHandle, read and write transaction interfaces
|
+-- backend-lmdb/ - LMDB implementation of the backend
|
+-- mvcc/         - multi-version key-value store
|                   revision-addressed records, key timeline index, SWMR read-write model
|
+-- wal/          - write-ahead log
|                   segmented append-only files, CRC per record, recovery
|
+-- lease/        - lease store
|                   TTL lifecycle, checkpoint replication, cascade delete on expiry
|
+-- consensus/    - Raft state machine and executor
|                   StoreWriter, StoreReader, SequentialExecutor
|
+-- server/       - gRPC server
|                   wires all modules together, KvServiceImpl, LeaseServiceImpl
|
+-- axisctl/      - command-line tool
                    interactive shell, single-command mode, cluster lifecycle management
```


## Quick start

**Prerequisites**: Java 25+, Maven 3.9+

Install prerequisites before running the setup script.

**macOS**
```bash
brew install --cask temurin@25
brew install maven
```

**Linux** - via [SDKMAN](https://sdkman.io)
```bash
curl -s "https://get.sdkman.io" | bash   # skip if already installed
sdk install java 25-tem
sdk install maven
```

**Windows**
```
winget install EclipseAdoptium.Temurin.25.JDK
winget install Apache.Maven
```

`setup.sh` requires Bash. On Windows, run it inside Git Bash which ships with [Git for Windows](https://git-scm.com/download/win).

**1. Run the setup script**

```bash
./setup.sh
```

Clones and installs the dependencies ([jaft](https://github.com/Rafee-Mohamed/jaft) and
[versioned-index](https://github.com/Rafee-Mohamed/versioned-index)) into the local Maven
repository, then builds Axis. The sources are discarded after installation.

**2. Add the alias to your shell profile**

```bash
alias axisctl='java \
  --enable-native-access=ALL-UNNAMED \
  --sun-misc-unsafe-memory-access=allow \
  -Dio.grpc.netty.shaded.io.netty.noUnsafe=true \
  -jar /path/to/axis/axisctl/target/axisctl-0.1.0-SNAPSHOT.jar'
```

The alias lets you type `axisctl` instead of the full `java -jar ...` invocation every
time. `--enable-native-access` is required by JLine's native terminal library.
`--sun-misc-unsafe-memory-access=allow` suppresses deprecated Unsafe warnings from
Protobuf and other bundled libraries on Java 25. The Netty flag prevents a separate
Unsafe warning from gRPC's shaded transport.

**3. Start a fresh 3-node cluster**

Axis stores all node data under `~/.axis/node-<id>/`. Wiping those directories before
starting gives a clean slate with no existing Raft log or data.

```bash
rm -rf ~/.axis/node-1 ~/.axis/node-2 ~/.axis/node-3
axisctl cluster start
sleep 6
axisctl cluster status
```

```
id      endpoint              status      pid
1       localhost:2379        running     12301
2       localhost:2479        running     12302
3       localhost:2579        running     12303
```

**4. Open the interactive shell**

```
$ axisctl
axisctl interactive shell
  commands : kv, lease, cluster
  help     : <command> --help
  quit     : exit or Ctrl+D
axis>
```

**5. Write and read keys**

```
axis> kv put a 1
stored  [rev:2]

axis> kv put b 2
stored  [rev:3]

axis> kv put c 3
stored  [rev:4]

axis> kv range a z
rev:4  3 results

+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 1     | 2        | 2       | 1       |
| b   | 2     | 3        | 3       | 1       |
| c   | 3     | 4        | 4       | 1       |
+-----+-------+----------+---------+---------+

axis> kv put a 100 --prev
stored  [rev:5]

prev:
+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 1     | 2        | 2       | 1       |
+-----+-------+----------+---------+---------+

axis> kv get a
rev:5

+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 100   | 5        | 2       | 2       |
+-----+-------+----------+---------+---------+
```

**6. Historical reads**

Every key carries the revision at which it was last modified (`MODIFIED`) and the
revision at which it was first created (`CREATED`). `--at` pins the read to any past
revision so the result reflects what the store looked like at that point.

```
axis> kv get a --at 2
rev:2

+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 1     | 2        | 2       | 1       |
+-----+-------+----------+---------+---------+

axis> kv range a z --at 3
rev:3  2 results

+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 1     | 2        | 2       | 1       |
| b   | 2     | 3        | 3       | 1       |
+-----+-------+----------+---------+---------+
```

**7. Leases**

A lease is a TTL-bound token. Keys attached to a lease are deleted atomically when
the lease expires or is revoked - useful for session tracking, distributed locks,
and ephemeral service registrations.

```
axis> lease grant 60
lease 736592719451906 granted with TTL(60s)

axis> kv put session token --lease 736592719451906
stored  [rev:6, lease:736592719451906]

axis> kv put lock owner-1 --lease 736592719451906
stored  [rev:7, lease:736592719451906]

axis> lease info 736592719451906
lease-id:  736592719451906
ttl:       60s
remaining: 55s
keys:      session, lock

axis> lease revoke 736592719451906
lease 736592719451906 revoked

axis> kv get session
rev:8
(not found)

axis> kv get lock
rev:8
(not found)
```

**8. Stop the cluster**

```bash
axisctl cluster stop
```

See [docs/axisctl.md](docs/axisctl.md) for the full CLI reference including all flags,
output format, and the complete command set.


## Documentation

- [docs/axisctl.md](docs/axisctl.md) - full CLI reference: all commands, flags, and output format
- [docs/api.md](docs/api.md) - gRPC API reference for KvService and LeaseService
- [docs/config.md](docs/config.md) - cluster topology and server configuration reference


## Roadmap

- **Snapshots** - Raft log compaction via `InstallSnapshot` is not implemented. The WAL
  grows unboundedly and every node replays the full log from the start on restart.
  Completing this requires snapshot serialization of MVCC and lease state, snapshot
  transfer between nodes, and WAL prefix truncation after a snapshot is applied.

- **Watch** - `WatchService` is defined in the proto but not implemented. The server needs
  event fanout from the state machine apply path, per-watch catch-up from a past revision,
  filter support (`NO_PUT`, `NO_DELETE`), and compaction-aware cancellation.

- **Tests** - No automated test suite exists. The system has been validated
  manually but there are no reproducible multi-node correctness or failure tests.

- **Observability** - No structured operational telemetry implemented yet.


## Components

These are not third-party libraries - they are purpose-built components developed as
part of the same effort and used directly by Axis.

**[Jaft](https://github.com/Rafee-Mohamed/jaft)** - the Raft consensus layer. Covers
leader election, log replication, membership changes, and the process-advance loop that
the `SequentialExecutor` drives.

**[versioned-index](https://github.com/Rafee-Mohamed/versioned-index)** - the
copy-on-write B+ tree used inside the MVCC layer as the in-memory key timeline index.


## Background

Axis is a project to learn and understand distributed systems and storage engine
fundamentals by building them from first principles - consensus, replication, 
coordination, consistency-models, mvcc, lease management etc.

## License

TBD
