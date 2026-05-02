# mvcc - Versioned Key-Value Store (Multi Version Concurrency Control)

The `mvcc` module is the versioned key-value store at the heart of Axis. Every version of every
key is retained, enabling access to historical states. It is designed as a single-writer,
multi-reader system where reads and writes never block each other. Each reader operates under
snapshot isolation, observing a consistent, immutable view of the store at creation time,
unaffected by concurrent or subsequent writes.


## The timeline

The timeline is the complete, ordered record of every mutation that has ever happened in the
store - across every key, from the beginning of time to now. It represents the store's entire
history as a single logical axis of time, where every write and every deletion has a permanent,
queryable position.

Rather than storing only the current value of a key, the store retains that full history -
every mutation indexed by a monotonically increasing logical clock called `commitSeq`.
This makes any past state queryable: a reader can ask "what did key `a` look like
at commit 6?" just as easily as asking for its current value.

Each key has its own slice of this timeline: an ordered sequence of revisions showing exactly
when it was created, when it was updated, when it was deleted, and whether it was recreated.
The timeline as a whole is the union of all these per-key histories, advancing forward with
every commit. Nothing is overwritten in place - new revisions are appended, and old ones
persist until compaction explicitly advances the visibility boundary.

The diagram below shows three keys across a shared timeline. Each row is one key's history;
the x-axis is `commitSeq`.

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
|           2          4          6          8         10         12         commitSeq   |
+----------------------------------------------------------------------------------------+

  ●  value written (live)     ○  key deleted (tombstone)     ->  key still alive
```

- `key a` is written at 2, updated at 6, deleted at 8, then recreated at 12 and still alive.
- `key b` is written at 4, updated at 10, and deleted at 12.
- `key c` is written at 6 and still alive.


## Concepts and terminology

### commitSeq

A `commitSeq` is a monotonically increasing integer that advances by one for every logical
commit. It is the store's logical clock. Every mutation is assigned the `commitSeq` of the
commit that produced it.

### Revision

A `Revision` is the unique identity of a single mutation: `(commitSeq, ordinal)`. When a
single commit writes multiple keys, each write gets the same `commitSeq` and a distinct
`ordinal` starting at zero.

```
commit 6: put(key-a), put(key-b), put(key-c)
  -> Revision(6, 0)  key-a
  -> Revision(6, 1)  key-b
  -> Revision(6, 2)  key-c
```

Revisions are totally ordered: compare `commitSeq` first, then `ordinal`.

### Span

A **span** is one continuous lifetime of a key - the sequence of revisions from creation to
deletion (or to "now" if still alive).

- A **live span** (`LiveSpan`) is open-ended: the key exists and has not been deleted.
- A **dead span** (`DeadSpan`) is closed: it ends with a tombstone revision marking deletion.

From the diagram, `key a` has two spans: a dead span covering commitSeq 2-8, and a live span
from commitSeq 12 onward. The gap between 8 and 12 is simply the key not existing.

### KeyTimeline

A `KeyTimeline` holds the full revision history of one key as an ordered sequence of dead
spans followed by a live span:

```
[ dead[0] ] [ dead[1] ] ... [ dead[n-1] ] [ live ]
  cs 2..8     cs 20..31       cs 50..60    cs 70..
```

The live span may be empty when the key is currently deleted.

### Record

A `Record` is the value payload stored at a specific revision. It carries:

| Field            | Meaning                                                                     |
|------------------|-----------------------------------------------------------------------------|
| `key`            | The key bytes                                                               |
| `val`            | The value bytes; empty for tombstones                                       |
| `tombstone`      | `true` if this revision marks a deletion                                    |
| `version`        | Count of modifications within the current span; starts at 1, resets on recreation |
| `createdAtSeq`   | `commitSeq` when this key was first created in its current span             |
| `modifiedAtSeq`  | `commitSeq` of the most recent write (including this one)                   |

### CommitSeqBound

The store maintains a mutable visibility window `[start, end]`:

```
  start                         end   next()
    |                            |      |
----+----------------------------+------+---->  commitSeq
    ^                            ^      ^
 compaction               last commit  in-progress commit
 boundary
```

- `start` - the compaction boundary; queries strictly below this return `Compacted`.
- `end` - the last committed `commitSeq`; all revisions at or below this are visible.
- `next()` = `end + 1` - the `commitSeq` the active writer uses for its mutations.


## Data model

Each revision record is stored in the backend under an encoded `Revision` key. The value is
an encoded `Record`. The `KeyTimelineIndex` holds a `KeyTimeline` per key in an in-memory
B+ tree; the backend holds the actual record bytes. Together they form the full MVCC store.

```
 Backend (persistent)                     KeyTimelineIndex (in-memory CoW B+ tree)
 +----------------------------------+     +------------------------------------------+
 |  Revision(2,0) -> Record(key-a)  |     |  key-a -> KeyTimeline                    |
 |  Revision(4,0) -> Record(key-b)  |     |             dead[0]: cs 2..8             |
 |  Revision(6,0) -> Record(key-a)  |     |             live:    cs 12..             |
 |  Revision(8,0) -> Record(key-a)  |     |  key-b -> KeyTimeline                    |
 |  ...                             |     |             dead[0]: cs 4..12            |
 +----------------------------------+     |  key-c -> KeyTimeline                    |
                                          |             live:    cs 6..              |
                                          +------------------------------------------+
```

A read at `commitSeq=7` for `key-a` consults the index: the timeline shows a live span
starting at 2, with the floor revision at or below 7 being `Revision(6,0)`. The backend
record at `Revision(6,0)` is returned.


## Write path

All mutations go through a `SessionWriter`. A session corresponds to a single backend write
transaction that accumulates multiple logical commits before being flushed.

```
  writer()
    |
    v
 SessionWriter.put(key, val)
    |  1. assign Revision(next(), ordinal++)
    |  2. add to KeyTimelineIndex via TimelineTxn
    |  3. stage RevisionRecord into RevisionRecordBuffer
    |  4. put encoded record into backend WriteTxn
    v
 Writer.close()  <- logical commit
    |  1. publish buffer (records visible to readers)
    |  2. commit TimelineTxn (index visible to readers)
    |  3. advance CommitSeqBound.end (readers now see this commitSeq)
```

A logical commit (`Writer.close()`) makes writes visible to new readers immediately - no
backend flush is required. The backend write transaction is held open across multiple logical
commits and flushed lazily by `sync()` or when the session expires.

### RevisionRecordBuffer

The `RevisionRecordBuffer` bridges logical commit visibility and backend persistence. Because
the backend transaction has not yet been flushed, records written in the current session only
exist in the buffer. Readers resolve records from the buffer first; if not found there, they
fall back to a backend read transaction.


## Read path

`reader()` creates a `CommitBoundedReader` pinned to the current committed state:

```
  reader()
    |  1. read CommitSeqBound.end -> lastCommitSeq  (volatile read - the visible commit horizon)
    |  2. pin buffer view at lastCommitSeq
    |  3. take KeyTimelineIndex view  (snapshot of the in-memory index)
    |  4. open backend ReadTxn  (snapshot of what is persisted)
    v
 CommitBoundedReader.get(key, commitSeq)
    |  1. query index view for the revision at commitSeq
    |  2. look up record in buffer first
    |  3. fall back to backend ReadTxn if not in buffer
```

`CommitSeqBound.end` is read first because the write path advances it last (after
`buffer.publish` and `index.commit`). Reading it first guarantees that the buffer and index
pinned in steps 2 and 3 have already reached `lastCommitSeq`. Reversing the order would allow
a reader to observe a `lastCommitSeq` higher than what the just-snapshotted index contains.


## Concurrency model

The store is a **single-writer, multiple-reader (SWMR)** system.

| Operation      | Thread safety                                  |
|----------------|------------------------------------------------|
| `reader()`     | Thread-safe; callable from any thread          |
| `writer()`     | Single writer only; not thread-safe            |
| `compact()`    | Single writer only; not thread-safe            |
| `sync()`       | Single writer only; not thread-safe            |

### How readers and the writer co-exist without locks

The writer and readers share three structures: the `RevisionRecordBuffer`, the
`KeyTimelineIndex`, and `CommitSeqBound.end`. All three use carefully ordered volatile
writes to establish happens-before without synchronization:

1. `buffer.publish()` - volatile write; makes staged records visible.
2. `tlTxn.commit()` - the B+ tree CoW update is atomic from readers' perspective.
3. `bound.advance()` - volatile write; readers that observe the new `end` are guaranteed to
   also see the buffer and index updates from steps 1 and 2.

A reader that observes `end = N` is guaranteed to see all mutations at or below `N` in both
the buffer and the index.


## Compaction

Compaction sets a new lower boundary on the visible history. Reads at any `commitSeq`
strictly below the boundary return `SnapshotResult.Compacted`.

Physical deletion is deferred: a `BatchCompactor` removes one batch of backend records per
`commit()`, draining the backlog incrementally to keep write latency flat.


## Recovery

On startup, `TimelineVersionedStore.restore()` replays all persisted revision records from
the backend in order, rebuilding the `KeyTimelineIndex` and recovering `CommitSeqBound`.

An optional `BiConsumer<Revision, Record>` observer is called once per revision record during
this replay, in revision order. This allows callers to reconstruct derived state (such as
lease-key attachments) in a single pass through the backend without a second scan.

```
TimelineVersionedStore.restore(backend, config, (revision, record) -> {
    leaseStore.keysRecoveryConsumer().accept(revision, record);
});
```

If a compaction batch was in progress at the time of the last shutdown, it is resumed before
the first write session opens, using the persisted `completedCompactionCommitSeq` marker to
distinguish a completed batch from one still in progress.


## Module structure

```
mvcc/
+-- store/    - public surface: VersionedStore, Reader, Writer interfaces
|               and TimelineVersionedStore, the primary implementation
|
+-- model/    - core domain types: Revision, Record, CommitSeqBound,
|               SnapshotResult, CompactResult, pagination/sort options
|
+-- timeline/ - per-key revision history: KeyTimeline, LiveSpan, DeadSpan,
|               KeyTimelineIndex (B+ tree), and transactional index access
|
+-- io/       - I/O layer: SessionWriter, CommitBoundedReader,
|               RevisionRecordBuffer, BatchCompactor
|
+-- codec/    - encoding/decoding Revision and Record to/from backend bytes
|
+-- internal/ - low-level concurrency primitives used by the timeline layer
```
