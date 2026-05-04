# API Reference

Axis exposes two gRPC services: `KvService` for key-value operations and `LeaseService`
for lease lifecycle management. A `WatchService` is defined in the proto but is not yet
implemented.

The service definitions live in `api/src/main/protobuf/`. Any gRPC client can be used
against these services - the Java stubs generated from the proto are in the `api` module.

---

## Common types

### KeyVal

Returned by read operations and `--prev` variants of writes and deletes.

| Field               | Type    | Description                                                        |
|---------------------|---------|--------------------------------------------------------------------|
| `key`               | bytes   | The key.                                                           |
| `val`               | bytes   | The value.                                                         |
| `created_revision`  | int64   | Store revision at which this key was first created in its current lifetime. Resets if the key is deleted and recreated. |
| `modified_revision` | int64   | Store revision of the most recent write to this key.               |
| `version`           | int64   | Number of writes to this key since it was created (or last recreated). Starts at 1. |
| `lease_id`          | int64   | ID of the attached lease, or 0 if no lease.                        |

### ResponseHeader

Every response includes a header with cluster context.

| Field        | Type  | Description                                              |
|--------------|-------|----------------------------------------------------------|
| `cluster_id` | int64 | Cluster identifier.                                      |
| `member_id`  | int64 | ID of the node that served this response.                |
| `revision`   | int64 | Store revision after this operation committed (writes), or the revision the read was served at (reads). |
| `raft_term`  | int64 | Raft term at the time of the response.                   |

### Historical read results

Operations ending in `At` can return one of three outcomes:

| Result      | Meaning                                                                  |
|-------------|--------------------------------------------------------------------------|
| `ok`        | The revision exists and is visible. Contains the query result.           |
| `compacted` | The requested revision has been compacted away. `first_visible_revision` is the oldest still available. |
| `future`    | The requested revision is ahead of the current store. `last_visible_revision` is the current head. |

### RangeOptions

Passed to `Range`, `RangeAt`, `Keys`, and `KeysAt` to filter and sort results.

| Field         | Type          | Description                                                          |
|---------------|---------------|----------------------------------------------------------------------|
| `limit`       | int64         | Maximum number of results to return. 0 means no limit. If the result is truncated, `more = true` in the response. |
| `modified_in` | RevisionRange | Filter to keys whose `modified_revision` falls in `[min, max]`.     |
| `created_in`  | RevisionRange | Filter to keys whose `created_revision` falls in `[min, max]`.      |
| `sort`        | SortOrder     | Sort target and direction (see below).                               |

**SortTarget**: `KEY`, `VERSION`, `CREATED_REVISION`, `MODIFIED_REVISION`, `VAL`

**SortDirection**: `ASCENDING` (default), `DESCENDING`

### CountOptions

Passed to `Count` and `CountAt`. Same filters as `RangeOptions` but without limit or sort.

| Field         | Type          | Description                                                          |
|---------------|---------------|----------------------------------------------------------------------|
| `modified_in` | RevisionRange | Filter to keys whose `modified_revision` falls in `[min, max]`.     |
| `created_in`  | RevisionRange | Filter to keys whose `created_revision` falls in `[min, max]`.      |

---

## Range convention

All range operations use a half-open interval `[from, to)`: `from` is inclusive, `to` is
exclusive. Keys are compared lexicographically on their raw bytes.

Examples:
- `from="a"  to="d"` matches `a`, `b`, `c` but not `d`
- `from="app" to="apq"` matches all keys starting with `app`
- `from=""   to="\xFF\xFF"` matches all keys

---

## KvService

### Reads

#### Get

```
rpc Get(GetRequest) returns (GetResponse)
```

Returns the current value of a key.

**Request**

| Field | Type  | Description  |
|-------|-------|--------------|
| `key` | bytes | Key to read. |

**Response**

| Field    | Type   | Description                              |
|----------|--------|------------------------------------------|
| `header` | ResponseHeader | -                              |
| `kv`     | KeyVal | Absent if the key does not exist.        |

---

#### GetAt

```
rpc GetAt(GetAtRequest) returns (GetAtResponse)
```

Returns the value of a key as it existed at a specific past revision.

**Request**

| Field      | Type  | Description                      |
|------------|-------|----------------------------------|
| `key`      | bytes | Key to read.                     |
| `revision` | int64 | Store revision to read at.       |

**Response** - `oneof result`

| Variant     | Type      | Description                                        |
|-------------|-----------|----------------------------------------------------|
| `ok`        | GetAtOk   | `kv` field contains the key-value; absent if the key did not exist at that revision. |
| `compacted` | Compacted | Requested revision is no longer available.         |
| `future`    | Future    | Requested revision has not been committed yet.     |

---

#### Range

```
rpc Range(RangeRequest) returns (RangeResponse)
```

Returns all keys in `[from, to)` at the current revision.

**Request**

| Field     | Type         | Description                            |
|-----------|--------------|----------------------------------------|
| `from`    | bytes        | Start of range (inclusive).            |
| `to`      | bytes        | End of range (exclusive).              |
| `options` | RangeOptions | Filtering, sorting, and pagination.    |

**Response**

| Field    | Type            | Description                                         |
|----------|-----------------|-----------------------------------------------------|
| `header` | ResponseHeader  | -                                                   |
| `kvs`    | repeated KeyVal | Keys in the range, in sort order (default: by key ascending). |
| `more`   | bool            | True if results were truncated by `options.limit`.  |

---

#### RangeAt

```
rpc RangeAt(RangeAtRequest) returns (RangeAtResponse)
```

Returns all keys in `[from, to)` as they existed at a specific past revision.

**Request**

| Field      | Type         | Description                         |
|------------|--------------|-------------------------------------|
| `from`     | bytes        | Start of range (inclusive).         |
| `to`       | bytes        | End of range (exclusive).           |
| `revision` | int64        | Store revision to read at.          |
| `options`  | RangeOptions | Filtering, sorting, and pagination. |

**Response** - `oneof result`

| Variant     | Type        | Description                                       |
|-------------|-------------|---------------------------------------------------|
| `ok`        | RangeResult | `kvs` and `more` fields.                         |
| `compacted` | Compacted   | Requested revision is no longer available.        |
| `future`    | Future      | Requested revision has not been committed yet.    |

---

#### Keys

```
rpc Keys(KeysRequest) returns (KeysResponse)
```

Returns only the keys (not values) in `[from, to)` at the current revision. Useful for
existence checks or key enumeration without transferring values.

**Request**

| Field     | Type         | Description                            |
|-----------|--------------|----------------------------------------|
| `from`    | bytes        | Start of range (inclusive).            |
| `to`      | bytes        | End of range (exclusive).              |
| `options` | RangeOptions | Filtering, sorting, and pagination.    |

**Response**

| Field    | Type           | Description                                             |
|----------|----------------|---------------------------------------------------------|
| `header` | ResponseHeader | -                                                       |
| `keys`   | repeated bytes | Keys in the range.                                      |
| `more`   | bool           | True if results were truncated by `options.limit`.      |

---

#### KeysAt

```
rpc KeysAt(KeysAtRequest) returns (KeysAtResponse)
```

Same as `Keys` but at a specific past revision. Returns `ok`, `compacted`, or `future`.

---

#### Count

```
rpc Count(CountRequest) returns (CountResponse)
```

Returns the number of keys in `[from, to)` at the current revision.

**Request**

| Field     | Type         | Description                            |
|-----------|--------------|----------------------------------------|
| `from`    | bytes        | Start of range (inclusive).            |
| `to`      | bytes        | End of range (exclusive).              |
| `options` | CountOptions | Filtering by revision ranges.          |

**Response**

| Field    | Type           | Description             |
|----------|----------------|-------------------------|
| `header` | ResponseHeader | -                       |
| `count`  | int64          | Number of matching keys.|

---

#### CountAt

```
rpc CountAt(CountAtRequest) returns (CountAtResponse)
```

Same as `Count` but at a specific past revision. Returns `ok`, `compacted`, or `future`.

---

### Writes

All writes are linearizable - they are serialized through Raft before being applied.

#### Put

```
rpc Put(PutRequest) returns (PutResponse)
```

Writes a key. Creates it if it does not exist; overwrites if it does.

**Request**: `key`, `val`

**Response**: `header` only.

---

#### PutAndGet

```
rpc PutAndGet(PutAndGetRequest) returns (PutAndGetResponse)
```

Writes a key and returns its previous value in the same operation.

**Request**: `key`, `val`

**Response**

| Field     | Type           | Description                                     |
|-----------|----------------|-------------------------------------------------|
| `header`  | ResponseHeader | -                                               |
| `prev_kv` | KeyVal         | The previous key-value. Absent if the key was new. |

---

#### PutWithLease

```
rpc PutWithLease(PutWithLeaseRequest) returns (PutWithLeaseResponse)
```

Writes a key and attaches it to a lease. The key is deleted automatically when the lease
expires or is revoked. Fails if the lease does not exist.

**Request**: `key`, `val`, `lease_id`

**Response** - `oneof result`

| Variant     | Type            | Description               |
|-------------|-----------------|---------------------------|
| `ok`        | PutWithLeaseOk  | Write succeeded.          |
| `not_found` | LeaseNotFound   | Lease does not exist. Key was not written. |

---

#### PutWithLeaseAndGet

```
rpc PutWithLeaseAndGet(PutWithLeaseAndGetRequest) returns (PutWithLeaseAndGetResponse)
```

Writes a key with a lease and returns the previous value.

**Request**: `key`, `val`, `lease_id`

**Response** - `oneof result`

| Variant     | Type                  | Description                                              |
|-------------|-----------------------|----------------------------------------------------------|
| `ok`        | PutWithLeaseAndGetOk  | `prev_kv` field contains the previous value; absent if the key was new. |
| `not_found` | LeaseNotFound         | Lease does not exist. Key was not written.               |

---

#### UpdateValue

```
rpc UpdateValue(UpdateValueRequest) returns (UpdateValueResponse)
```

Updates the value of an existing key while preserving its lease attachment. Not an upsert -
fails if the key does not exist.

**Request**: `key`, `val`

**Response** - `oneof result`

| Variant         | Type            | Description              |
|-----------------|-----------------|--------------------------|
| `ok`            | UpdateValueOk   | Update succeeded.        |
| `key_not_found` | KeyNotFound     | Key does not exist.      |

---

#### UpdateLease

```
rpc UpdateLease(UpdateLeaseRequest) returns (UpdateLeaseResponse)
```

Moves an existing key to a different lease. Detaches from the previous lease and attaches
to the new one. Both the key and the new lease must exist.

**Request**: `key`, `lease_id`

**Response** - `oneof result`

| Variant           | Type            | Description                         |
|-------------------|-----------------|-------------------------------------|
| `ok`              | UpdateLeaseOk   | Update succeeded.                   |
| `key_not_found`   | KeyNotFound     | Key does not exist.                 |
| `lease_not_found` | LeaseNotFound   | New lease does not exist.           |

---

#### RemoveLease

```
rpc RemoveLease(RemoveLeaseRequest) returns (PutResponse)
```

Detaches a key from its lease, turning it into a plain key with no expiry. If the key has
no lease this is a no-op. The key itself is not deleted.

**Request**: `key`

**Response**: `header` only.

---

#### Delete

```
rpc Delete(DeleteRequest) returns (DeleteResponse)
```

Deletes a single key.

**Request**: `key`

**Response**

| Field     | Type           | Description                               |
|-----------|----------------|-------------------------------------------|
| `header`  | ResponseHeader | -                                         |
| `deleted` | bool           | True if the key existed and was deleted.  |

---

#### DeleteAndGet

```
rpc DeleteAndGet(DeleteAndGetRequest) returns (DeleteAndGetResponse)
```

Deletes a single key and returns its previous value.

**Request**: `key`

**Response**

| Field     | Type           | Description                                       |
|-----------|----------------|---------------------------------------------------|
| `header`  | ResponseHeader | -                                                 |
| `prev_kv` | KeyVal         | The deleted key-value. Absent if key did not exist. |

---

#### DeleteRange

```
rpc DeleteRange(DeleteRangeRequest) returns (DeleteRangeResponse)
```

Deletes all keys in `[from, to)`.

**Request**: `from`, `to`

**Response**

| Field     | Type           | Description                         |
|-----------|----------------|-------------------------------------|
| `header`  | ResponseHeader | -                                   |
| `deleted` | int64          | Number of keys deleted.             |

---

#### DeleteRangeAndGet

```
rpc DeleteRangeAndGet(DeleteRangeAndGetRequest) returns (DeleteRangeAndGetResponse)
```

Deletes all keys in `[from, to)` and returns their previous values.

**Request**: `from`, `to`

**Response**

| Field      | Type            | Description                           |
|------------|-----------------|---------------------------------------|
| `header`   | ResponseHeader  | -                                     |
| `prev_kvs` | repeated KeyVal | The deleted key-values.               |

---

### Compact

```
rpc Compact(CompactRequest) returns (CompactResponse)
```

Advances the compaction boundary to the given revision. All history strictly before that
revision is discarded and can no longer be queried. Reads at or after the boundary are
unaffected.

**Request**: `revision`

**Response**: `header` only.

Use compaction to reclaim storage after history is no longer needed. Deletion is
incremental - old records are not removed immediately.

---

### Txn

```
rpc Txn(TxnRequest) returns (TxnResponse)
```

Executes a transaction atomically. The transaction evaluates a list of conditions, then
executes the `success` ops if all conditions pass or the `failure` ops if any condition
fails. Conditions, success, and failure lists may all be empty.

#### TxnRequest

| Field        | Type               | Description                                                    |
|--------------|--------------------|----------------------------------------------------------------|
| `conditions` | repeated Condition | All must pass for the success branch to run.                   |
| `success`    | repeated TxnOp     | Ops executed when all conditions pass.                         |
| `failure`    | repeated TxnOp     | Ops executed when any condition fails.                         |

#### Condition

A condition targets either a single key or a key range, compares one of its attributes
against an expected value, and evaluates to true or false.

**Scope** - `oneof scope`

| Variant | Description                                           |
|---------|-------------------------------------------------------|
| `key`   | Target a single key.                                  |
| `range` | Target a key range `[from, to)`. The condition passes only if all keys in the range satisfy it. An empty range passes. |

**Target** - `oneof target`

| Variant             | Type  | Description                                  |
|---------------------|-------|----------------------------------------------|
| `version`           | int64 | Current version of the key.                  |
| `created_revision`  | int64 | Revision at which the key was created.       |
| `modified_revision` | int64 | Revision at which the key was last modified. |
| `val`               | bytes | Current value of the key.                    |
| `lease_id`          | int64 | ID of the lease currently attached.          |

**CompareOp**: `EQ`, `NE`, `GT`, `LT`, `GTE`, `LTE`

If the key does not exist, `version`, `created_revision`, `modified_revision`, and
`lease_id` compare as 0, and `val` compares as empty bytes.

#### TxnOp

Ops inside a transaction support the full read and write surface, plus nested transactions:

**Reads**: `Get`, `GetAt`, `Range`, `RangeAt`, `Keys`, `KeysAt`, `Count`, `CountAt`

**Writes**: `Put`, `PutAndGet`, `PutWithLease`, `PutWithLeaseAndGet`, `UpdateValue`,
`UpdateLease`, `RemoveLease`, `Delete`, `DeleteAndGet`, `DeleteRange`, `DeleteRangeAndGet`

**Nested**: `Txn`

#### TxnResponse

| Field       | Type                | Description                                                       |
|-------------|---------------------|-------------------------------------------------------------------|
| `header`    | ResponseHeader      | -                                                                 |
| `succeeded` | bool                | True if all conditions passed and the success branch ran.         |
| `results`   | repeated TxnOpResult | One result per op in the branch that ran, in the same order.    |

Each `TxnOpResult` carries the same response type as the corresponding standalone call.
A write that fails within a transaction (for example `PutWithLease` with a missing lease)
returns its own result with the failure variant set - it does not abort the transaction.

#### Example

Check that key `lock` does not exist, then write it atomically:

```
TxnRequest {
    conditions: [
        Condition {
            key: "lock",
            version: 0,
            op: EQ
        }
    ]
    success: [
        TxnOp { put: PutRequest { key: "lock", val: "owner-1" } }
    ]
    failure: [
        TxnOp { get: GetRequest { key: "lock" } }
    ]
}
```

If `lock` does not exist (`version == 0`), it is written and `succeeded = true`.
If it already exists, the current value is returned in the failure result and
`succeeded = false`.

---

## LeaseService

### Grant

```
rpc Grant(GrantRequest) returns (GrantResponse)
```

Creates a new lease with the given TTL.

**Request**

| Field      | Type  | Description                                                       |
|------------|-------|-------------------------------------------------------------------|
| `ttl`      | int64 | Time-to-live in seconds. Must be >= `lease.min-ttl` in config.   |
| `lease_id` | int64 | Requested lease ID. Pass 0 to let the server assign one.         |

**Response**

| Field      | Type           | Description                           |
|------------|----------------|---------------------------------------|
| `header`   | ResponseHeader | -                                     |
| `lease_id` | int64          | The assigned lease ID.                |
| `ttl`      | int64          | Granted TTL in seconds.               |

---

### Revoke

```
rpc Revoke(RevokeRequest) returns (RevokeResponse)
```

Revokes a lease and cascade-deletes all keys attached to it.

**Request**: `lease_id`

**Response** - `oneof result`

| Variant     | Type         | Description                        |
|-------------|--------------|------------------------------------|
| `revoked`   | LeaseRevoked | Lease was revoked.                 |
| `not_found` | LeaseNotFound| Lease does not exist.              |

---

### Renew

```
rpc Renew(stream RenewRequest) returns (stream RenewResponse)
```

Bidirectional streaming RPC for keeping leases alive. The client sends periodic
`RenewRequest` messages; the server responds with `RenewResponse` for each.

**RenewRequest**: `lease_id`

**RenewResponse** - `oneof result`

| Variant     | Type          | Description                                            |
|-------------|---------------|--------------------------------------------------------|
| `renewed`   | LeaseRenewed  | Lease is alive. `remaining_ttl` reflects the new TTL. |
| `not_found` | LeaseNotFound | Lease expired or was revoked. Client should stop sending for this lease. |

Send one `RenewRequest` per lease per heartbeat interval. A common interval is
`ttl / 3` seconds to keep well ahead of expiry.

---

### Info

```
rpc Info(InfoRequest) returns (InfoResponse)
```

Returns full details for a single lease including all attached keys.

**Request**: `lease_id`

**Response** - `oneof result`

| Variant     | Type         | Description                            |
|-------------|--------------|----------------------------------------|
| `found`     | LeaseDetail  | Full lease record (see below).         |
| `not_found` | LeaseNotFound| Lease does not exist.                  |

**LeaseDetail**

| Field          | Type           | Description                              |
|----------------|----------------|------------------------------------------|
| `lease_id`     | int64          | -                                        |
| `ttl`          | int64          | Original TTL granted in seconds.         |
| `remaining_ttl`| int64          | Remaining TTL at the time of the call.   |
| `keys`         | repeated bytes | All keys currently attached to this lease. |

---

### Leases

```
rpc Leases(LeasesRequest) returns (LeasesResponse)
```

Returns a summary of all active leases. Does not include attached keys - use `Info` for
a specific lease's keys.

**Request**: empty

**Response**

| Field    | Type                | Description                                  |
|----------|---------------------|----------------------------------------------|
| `header` | ResponseHeader      | -                                            |
| `leases` | repeated LeaseInfo  | One entry per active lease.                  |

**LeaseInfo**: `lease_id`, `ttl`, `remaining_ttl`

---

## WatchService

The `WatchService` proto is defined and describes a bidirectional streaming watch
interface for subscribing to key and range change events. It is not yet implemented
in the server.

See `api/src/main/protobuf/watch.proto` for the full message definitions.
