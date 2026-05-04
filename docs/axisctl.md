# axisctl

Command-line tool for interacting with an Axis KV cluster. Supports key-value operations,
lease management, and cluster lifecycle control.

---

## Build

From the repository root:

```bash
mvn install -DskipTests -q
```

Fat jar: `axisctl/target/axisctl-0.1.0-SNAPSHOT.jar`

To rebuild only axisctl after changes:

```bash
mvn -pl axisctl package -DskipTests -q
```

---

## Alias

Add to your shell profile (`~/.zshrc` or `~/.bashrc`):

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
Replace `/path/to/axis` with the actual path to your clone.

---

## Interactive shell

Running `axisctl` with no arguments starts an interactive session:

```
$ axisctl
axisctl interactive shell
  commands : kv, lease, cluster
  help     : <command> --help
  quit     : exit or Ctrl+D
axis>
```

Tab completion is available for all commands and flags. Ctrl+C clears the current line;
Ctrl+D or `exit` ends the session.

Single-command mode works the same - `axisctl kv put foo bar` runs and exits.

---

## Output conventions

**Revision line**

Every write response includes a revision:

```
stored  [rev:42]
stored  [rev:43, lease:736592719451906]
updated  [rev:44]
deleted  [rev:45]
```

`rev:N` is the store-wide revision after the operation committed. Each write increments
it by one. Reads also show the revision they were served at.

**Key-value table**

Read operations (`get`, `range`) return an ASCII table:

```
rev:5  3 results

+-------+-------+----------+---------+---------+
| KEY   | VALUE | MODIFIED | CREATED | VERSION |
+-------+-------+----------+---------+---------+
| a     | 1     | 2        | 2       | 1       |
| b     | 2     | 3        | 3       | 1       |
| c     | 3     | 5        | 5       | 1       |
+-------+-------+----------+---------+---------+
```

| Column     | Meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| `MODIFIED` | The revision at which this key was last written                         |
| `CREATED`  | The revision at which this key was first created in its current lifetime |
| `VERSION`  | Number of writes to this key since it was created (or last recreated)   |

If any key in the result has a lease, a `LEASE` column is added. Keys without a lease
show `-` in that column:

```
+-----+-------+----------+---------+---------+--------------------+
| KEY | VALUE | MODIFIED | CREATED | VERSION | LEASE              |
+-----+-------+----------+---------+---------+--------------------+
| foo | bar   | 8        | 8       | 1       | 736592719451906    |
| baz | qux   | 6        | 6       | 1       | -                  |
+-----+-------+----------+---------+---------+--------------------+
```

**prev: block**

Commands with `--prev` show the key's state before the operation:

```
stored  [rev:10]

prev:
+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| foo | old   | 4        | 2       | 3       |
+-----+-------+----------+---------+---------+
```

If the key had no previous value: `prev: (none)`

---

## Common flag: `--endpoint`

All `kv` and `lease` commands accept:

```
-e, --endpoint HOST:PORT   (default: localhost:2379)
```

Use this to target a specific node when the cluster is running on non-default ports or
on remote hosts.

```bash
axisctl kv get foo --endpoint localhost:2479
axisctl lease list -e 10.0.0.2:2379
```

---

## kv commands

### `kv put KEY VALUE`

Writes a key. Creates it if it does not exist; overwrites if it does.

```
Flags:
  --lease LEASE_ID   Attach the key to a lease. The key is deleted automatically
                     when the lease expires or is revoked.
  --prev             Return the previous value before this write.
```

```bash
axisctl kv put foo bar
# stored  [rev:5]

axisctl kv put foo bar2 --prev
# stored  [rev:6]
#
# prev:
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | foo | bar   | 5        | 5       | 1       |
# +-----+-------+----------+---------+---------+

axisctl kv put foo bar3 --prev
# stored  [rev:7]
#
# prev:
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | foo | bar2  | 6        | 5       | 2       |
# +-----+-------+----------+---------+---------+
# Note: CREATED is still 5 (same lifetime), VERSION incremented to 2

axisctl kv put session abc --lease 736592719451906
# stored  [rev:8, lease:736592719451906]

axisctl kv put session abc --lease 999
# lease 999 not found
```

---

### `kv get KEY`

Reads the current value of a key.

```
Flags:
  --at REVISION   Read the key as it existed at a specific past revision.
```

```bash
axisctl kv get foo
# rev:8
#
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | foo | bar3  | 7        | 5       | 3       |
# +-----+-------+----------+---------+---------+

axisctl kv get missing
# rev:8
# (not found)

axisctl kv get foo --at 6
# rev:6
#
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | foo | bar2  | 6        | 5       | 2       |
# +-----+-------+----------+---------+---------+

axisctl kv get foo --at 3
# (compacted - rev:3 is gone, oldest available: 5)

axisctl kv get foo --at 100
# (future - rev:100 not yet committed, current: 8)
```

`MODIFIED` and `CREATED` show the revision values as of the requested revision, not the
current ones - the table reflects the key's state at that point in time.

---

### `kv del FROM [TO]`

Deletes a single key or a range of keys `[FROM, TO)`.

```
Flags:
  --prev   Return the deleted key(s) before deletion.
```

```bash
# Single key
axisctl kv del foo
# deleted  [rev:9]

axisctl kv del missing
# not-found  [rev:9]

axisctl kv del foo --prev
# deleted  [rev:10]
#
# prev:
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | foo | bar3  | 7        | 5       | 3       |
# +-----+-------+----------+---------+---------+

# Range delete [a, d) - deletes a, b, c but not d
axisctl kv del a d
# deleted  3  [rev:11]

axisctl kv del a d --prev
# deleted  3  [rev:12]
#
# prev:
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | a   | 1     | 2        | 2       | 1       |
# | b   | 2     | 3        | 3       | 1       |
# | c   | 3     | 5        | 5       | 1       |
# +-----+-------+----------+---------+---------+
```

Range delete follows the same `[FROM, TO)` convention as `range` - `TO` is exclusive.
If `TO` is omitted, only the single key `FROM` is deleted.

---

### `kv range FROM TO`

Lists all keys in `[FROM, TO)` in lexicographic order. `TO` is exclusive.

```
Flags:
  --at REVISION   Read the range as it existed at a specific past revision.
```

```bash
axisctl kv range a d
# rev:5  3 results
#
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | a   | 1     | 2        | 2       | 1       |
# | b   | 2     | 3        | 3       | 1       |
# | c   | 3     | 5        | 5       | 1       |
# +-----+-------+----------+---------+---------+

axisctl kv range a z
# Returns every key between a (inclusive) and z (exclusive)

axisctl kv range a d --at 3
# rev:3  2 results  - only a and b existed at rev:3
#
# +-----+-------+----------+---------+---------+
# | KEY | VALUE | MODIFIED | CREATED | VERSION |
# +-----+-------+----------+---------+---------+
# | a   | 1     | 2        | 2       | 1       |
# | b   | 2     | 3        | 3       | 1       |
# +-----+-------+----------+---------+---------+

axisctl kv range x z
# rev:5
# (empty)
```

If the output line includes `(more available)`, the result was truncated by the server's
page limit. Use the gRPC API directly for paginated access to large ranges.

---

### `kv update-value KEY VALUE`

Updates the value of an existing key while preserving its lease attachment. Fails if the
key does not exist - this is not an upsert.

```bash
axisctl kv update-value counter 42
# updated  [rev:12]

axisctl kv update-value missing 42
# not-found  [rev:12]
```

Use `put` to create or overwrite a key unconditionally. Use `update-value` when you want
to guarantee you are updating something that already exists without disturbing its lease.

---

### `kv update-lease KEY LEASE_ID`

Moves an existing key to a different lease. Both the key and the new lease must exist.
The key is detached from its previous lease and attached to the new one.

```bash
axisctl kv update-lease session 812345678901234
# updated  [rev:13]

axisctl kv update-lease missing 812345678901234
# not-found  [rev:13]

axisctl kv update-lease session 999
# lease 999 not found
```

---

### `kv remove-lease KEY`

Detaches a key from its lease, turning it into a plain key with no expiry. If the key has
no lease this is a no-op. The key itself is not deleted.

```bash
axisctl kv remove-lease session
# updated  [rev:14]
```

---

## lease commands

### `lease grant TTL`

Creates a new lease. `TTL` is the time-to-live in seconds. Returns the assigned lease ID.

```bash
axisctl lease grant 60
# lease 736592719451906 granted with TTL(60s)
```

The lease ID is a large integer generated by the cluster. Use it in subsequent `kv put
--lease`, `update-lease`, and `lease info/revoke` calls.

---

### `lease list`

Lists all active leases. Does not include attached keys - use `lease info` for that.

```bash
axisctl lease list
# 2 leases
#
# +--------------------+--------+-------------+
# | LEASE-ID           | TTL(s) | REMAINING(s)|
# +--------------------+--------+-------------+
# | 736592719451906    | 60     | 47          |
# | 812345678901234    | 120    | 98          |
# +--------------------+--------+-------------+

axisctl lease list
# (no active leases)
```

`REMAINING(s)` reflects the current remaining TTL at the time of the request.

---

### `lease info LEASE_ID`

Shows full details for a single lease including all keys currently attached to it.

```bash
axisctl lease info 736592719451906
# lease-id:  736592719451906
# ttl:       60s
# remaining: 41s
# keys:      session, token

axisctl lease info 736592719451906
# lease-id:  736592719451906
# ttl:       60s
# remaining: 38s
# keys:      (none)

axisctl lease info 999
# lease 999 not found
```

---

### `lease revoke LEASE_ID`

Revokes a lease and cascade-deletes all keys attached to it.

```bash
axisctl lease revoke 736592719451906
# lease 736592719451906 revoked

axisctl lease revoke 999
# lease 999 not found
```

---

## cluster commands

### `cluster start [TOPOLOGY]`

Starts all nodes defined in the topology. If no topology file is given and `cluster.yaml`
is not present in the current directory, the bundled default 3-node local topology is used.

```
Flags:
  --node NODE_ID   Start only this node (default: start all).
```

```bash
axisctl cluster start
# node 1 started (pid 12345) - log: /Users/you/.axis/node-1/server.log
# node 2 started (pid 12346) - log: /Users/you/.axis/node-2/server.log
# node 3 started (pid 12347) - log: /Users/you/.axis/node-3/server.log

axisctl cluster start my-cluster.yaml
# node 1 started (pid 12345) - log: ...

axisctl cluster start --node 2
# node 2 started (pid 12346) - log: /Users/you/.axis/node-2/server.log
```

Default topology ports:

| Node | Client port | Peer port |
|------|-------------|-----------|
| 1    | 2379        | 2380      |
| 2    | 2479        | 2480      |
| 3    | 2579        | 2580      |

---

### `cluster stop [TOPOLOGY]`

Stops all running nodes, or a specific node with `--node`.

```
Flags:
  --node NODE_ID   Stop only this node.
```

```bash
axisctl cluster stop
# node 1 stopped (pid 12345)
# node 2 stopped (pid 12346)
# node 3 stopped (pid 12347)

axisctl cluster stop my-cluster.yaml
```
A node in `dead` state has a PID file but the process is no longer alive - this happens
when a node crashes without going through `cluster stop`. Running `cluster start` again
will overwrite the stale PID with the new process. To clear it without restarting, remove
`~/.axis/node-<id>/axis.pid` manually.

---

### `cluster status [TOPOLOGY]`

Shows the running state of each node.

```bash
axisctl cluster status
# id      endpoint              status      pid
# 1       localhost:2379        running     12345
# 2       localhost:2479        running     12346
# 3       localhost:2579        stopped     -

axisctl cluster status my-cluster.yaml
```

---

### `cluster logs [TOPOLOGY]`

Shows recent log output for cluster nodes.

```
Flags:
  --node NODE_ID    Show logs for a specific node only (default: all).
  -n LINES          Number of recent lines per node (default: 50).
  -f, --follow      Stream log output continuously (Ctrl-C to stop).
```

```bash
axisctl cluster logs --node 1 -n 20
axisctl cluster logs -f
axisctl cluster logs -f --node 1
```

---

## The `.axis` directory

All node data lives under `~/.axis/`:

```
~/.axis/
  node-1/
    lmdb/        ← persistent key-value storage
    wal/         ← Raft write-ahead log
    server.log   ← stdout/stderr for this node
    axis.pid     ← process ID; written on start, removed on stop
  node-2/
  node-3/
```

**Starting fresh** - wipe the node data directories:

```bash
rm -rf ~/.axis/node-1 ~/.axis/node-2 ~/.axis/node-3
axisctl cluster start
```

**Resuming** - just run `axisctl cluster start`. Nodes detect existing data and resume
from their last committed state without any extra steps.

---

## End-to-end walkthrough

Start and stop the cluster from your shell. Everything in between runs inside the
interactive session.

**Start a fresh cluster:**

```bash
$ rm -rf ~/.axis/node-1 ~/.axis/node-2 ~/.axis/node-3
$ axisctl cluster start
node 1 started (pid 12301) - log: /Users/you/.axis/node-1/server.log
node 2 started (pid 12302) - log: /Users/you/.axis/node-2/server.log
node 3 started (pid 12303) - log: /Users/you/.axis/node-3/server.log
$ sleep 6
$ axisctl cluster status
id      endpoint              status      pid
1       localhost:2379        running     12301
2       localhost:2479        running     12302
3       localhost:2579        running     12303
```

**Open the interactive session:**

```
$ axisctl
axisctl interactive shell
  commands : kv, lease, cluster
  help     : <command> --help
  quit     : exit or Ctrl+D
axis>
```

**Write keys and read them back:**

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
```

**Update a key and observe versioning:**

```
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

CREATED is still 2 (same lifetime), MODIFIED updated to 5, VERSION incremented to 2.

**Historical read:**

```
axis> kv get a --at 2
rev:2

+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 1     | 2        | 2       | 1       |
+-----+-------+----------+---------+---------+
```

**Lease round-trip:**

```
axis> lease grant 30
lease 736592719451906 granted with TTL(30s)

axis> lease grant 120
lease 812345678901234 granted with TTL(120s)

axis> lease list
2 leases

+--------------------+--------+-------------+
| LEASE-ID           | TTL(s) | REMAINING(s)|
+--------------------+--------+-------------+
| 736592719451906    | 30     | 29          |
| 812345678901234    | 120    | 119         |
+--------------------+--------+-------------+

axis> kv put session abc --lease 736592719451906
stored  [rev:6, lease:736592719451906]

axis> lease info 736592719451906
lease-id:  736592719451906
ttl:       30s
remaining: 27s
keys:      session
```

**Move key to the longer-lived lease:**

```
axis> kv update-lease session 812345678901234
updated  [rev:7]

axis> lease list
2 leases

+--------------------+--------+-------------+
| LEASE-ID           | TTL(s) | REMAINING(s)|
+--------------------+--------+-------------+
| 736592719451906    | 30     | 22          |
| 812345678901234    | 120    | 114         |
+--------------------+--------+-------------+

axis> lease info 736592719451906
lease-id:  736592719451906
ttl:       30s
remaining: 22s
keys:      (none)
```

`session` is now owned by the 120s lease. The 30s lease still exists but has no keys attached.

**Revoke and confirm cascade delete:**

```
axis> lease revoke 812345678901234
lease 812345678901234 revoked

axis> kv get session
rev:8
(not found)
```

**Delete a range and inspect what was removed:**

```
axis> kv del b
deleted  [rev:9]

axis> kv del a z --prev
deleted  2  [rev:10]

prev:
+-----+-------+----------+---------+---------+
| KEY | VALUE | MODIFIED | CREATED | VERSION |
+-----+-------+----------+---------+---------+
| a   | 100   | 5        | 2       | 2       |
| c   | 3     | 4        | 4       | 1       |
+-----+-------+----------+---------+---------+
```

`b` was already gone so the range only returned 2 entries.

**Exit the session:**

```
axis> exit
```

**Stop the cluster:**

```bash
$ axisctl cluster stop
node 1 stopped (pid 12301)
node 2 stopped (pid 12302)
node 3 stopped (pid 12303)
```

---

## Known limitations

- **Transactions**: `txn` is not supported in axisctl. Use the gRPC API directly for
  multi-key atomic operations with conditions.
