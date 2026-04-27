package io.disys.axis.mvcc.store;

import io.disys.axis.backend.WriteHandle;
import io.disys.axis.mvcc.model.Record;

import java.util.List;
import java.util.Optional;

/**
 * A single-writer, single-commit unit of work over the MVCC store.
 *
 * <p>Extends {@link Reader}: all read operations ({@link #get}, {@link #range},
 * and siblings) always query at the writer's current commitSeq with
 * read-your-write semantics; writes staged via {@link #put}, {@link #delete},
 * and {@link #deleteRange} are immediately visible to subsequent reads within
 * the same Writer.</p>
 *
 * <p>{@link #close()} is the logical commit: staged writes are visible to other
 * readers after this call returns. No guarantees are made about persistence or
 * durability - those are outside this contract.</p>
 */
public interface Writer extends Reader, AutoCloseable {

    /**
     * Stages a write for {@code key} at the writer's commitSeq. Creates the key if absent,
     * updates it if present. The write is immediately visible to this writer's read operations.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +--------------+---------------------+--------------------------+
     *   | operation    | result              | note                     |
     *   +--------------+---------------------+--------------------------+
     *   | put(a, v4)   |                     | a(v3) at seq 15; updated |
     *   +--------------+---------------------+--------------------------+
     *   | get(a)       | of(Record(a, v4))   | read-your-write          |
     *   +--------------+---------------------+--------------------------+
     *   | put(d, v1)   |                     | d absent; created        |
     *   +--------------+---------------------+--------------------------+
     *   | get(d)       | of(Record(d, v1))   | read-your-write          |
     *   +--------------+---------------------+--------------------------+
     *
     * after close():
     *   +-----+------------------------+
     *   | seq |          16            |
     *   +-----+------------------------+
     *   | op  | put(a, v4); put(d, v1) |
     *   +-----+------------------------+
     * </pre>
     *
     * @param key the key to write
     * @param val the value to associate with the key
     */
    void put(byte[] key, byte[] val);

    /**
     * Returns the record for {@code key} as it stood before this put, then stages the write.
     * Empty if the key was absent.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +--------------------+---------------------+----------------------+
     *   | operation          | result              | note                 |
     *   +--------------------+---------------------+----------------------+
     *   | putAndGet(a, v4)   | of(Record(a, v3))   | a(v3) at seq 15      |
     *   +--------------------+---------------------+----------------------+
     *   | putAndGet(d, v1)   | empty()             | d absent             |
     *   +--------------------+---------------------+----------------------+
     *
     * after close():
     *   +-----+------------------------+
     *   | seq |          16            |
     *   +-----+------------------------+
     *   | op  | put(a, v4); put(d, v1) |
     *   +-----+------------------------+
     * </pre>
     *
     * @param key the key to write
     * @param val the value to associate with the key
     * @return the record as it stood before this put, or empty if the key was absent
     */
    Optional<Record> putAndGet(byte[] key, byte[] val);

    /**
     * Stages a tombstone for {@code key}. Returns {@code false} and makes no change
     * if the key is absent.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +-------------+----------+------------------------------+
     *   | operation   | result   | note                         |
     *   +-------------+----------+------------------------------+
     *   | delete(a)   | true     | a(v3) present at seq 15      |
     *   +-------------+----------+------------------------------+
     *   | get(a)      | empty()  | deleted; read-your-write     |
     *   +-------------+----------+------------------------------+
     *   | delete(b)   | false    | b absent (deleted at seq 14) |
     *   +-------------+----------+------------------------------+
     *   | delete(d)   | false    | d absent                     |
     *   +-------------+----------+------------------------------+
     *
     * after close():
     *   +-----+--------+
     *   | seq |   16   |
     *   +-----+--------+
     *   | op  | del(a) |
     *   +-----+--------+
     * </pre>
     *
     * @param key the key to delete
     * @return true if the key existed and was deleted, false if absent
     */
    boolean delete(byte[] key);

    /**
     * Returns the record for {@code key} as it stood before this delete, then stages
     * the tombstone. Returns empty and makes no change if the key is absent.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +------------------+---------------------+----------------------+
     *   | operation        | result              | note                 |
     *   +------------------+---------------------+----------------------+
     *   | deleteAndGet(a)  | of(Record(a, v3))   | a(v3) at seq 15      |
     *   +------------------+---------------------+----------------------+
     *   | deleteAndGet(d)  | empty()             | d absent; no-op      |
     *   +------------------+---------------------+----------------------+
     *
     * after close():
     *   +-----+--------+
     *   | seq |   16   |
     *   +-----+--------+
     *   | op  | del(a) |
     *   +-----+--------+
     * </pre>
     *
     * @param key the key to delete
     * @return the record before this delete, or empty if the key was absent
     */
    Optional<Record> deleteAndGet(byte[] key);

    /**
     * Stages tombstones for all present keys in {@code [from, to)}.
     * Returns the count of keys actually deleted; absent keys in the range are not counted.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +-------------------+----------+--------------------------------+
     *   | operation         | result   | note                           |
     *   +-------------------+----------+--------------------------------+
     *   | deleteRange(a, d) | 2        | a(v3), c(v1) present; b absent |
     *   +-------------------+----------+--------------------------------+
     *   | deleteRange(b, c) | 0        | b absent; no keys in range     |
     *   +-------------------+----------+--------------------------------+
     *
     * after close():
     *   +-----+------------------+
     *   | seq |        16        |
     *   +-----+------------------+
     *   | op  | del(a); del(c)   |
     *   +-----+------------------+
     * </pre>
     *
     * @param from the inclusive lower bound
     * @param to   the exclusive upper bound
     * @return the count of keys deleted
     */
    int deleteRange(byte[] from, byte[] to);

    /**
     * Returns the records for all present keys in {@code [from, to)} as they stood before
     * deletion, then stages tombstones for each. Absent keys are not included in the result.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     * writer commitSeq: 16
     *
     *   +---------------------------+----------------+------------------+
     *   | operation                 | result         | note             |
     *   +---------------------------+----------------+------------------+
     *   | deleteRangeAndGet(a, d)   | [a(v3), c(v1)] | a and c present  |
     *   +---------------------------+----------------+------------------+
     *   | deleteRangeAndGet(b, c)   | []             | b absent         |
     *   +---------------------------+----------------+------------------+
     *
     * after close():
     *   +-----+------------------+
     *   | seq |        16        |
     *   +-----+------------------+
     *   | op  | del(a); del(c)   |
     *   +-----+------------------+
     * </pre>
     *
     * @param from the inclusive lower bound
     * @param to   the exclusive upper bound
     * @return records as they stood before deletion; empty if no keys were present in the range
     */
    List<Record> deleteRangeAndGet(byte[] from, byte[] to);

    /**
     * Returns the commitSeq at which staged writes will land. Returns the last committed
     * commitSeq if no writes have been staged, the same value as {@link Reader#revision()}
     * in that case.
     *
     * <pre>
     * store:
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | seq |     2      |     4      |     6      |     8      |   10   |    12      |   14   |    15      |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *   | op  | put(a, v1) | put(b, v1) | put(a, v2) | put(c, v1) | del(a) | put(b, v2) | del(b) | put(a, v3) |
     *   +-----+------------+------------+------------+------------+--------+------------+--------+------------+
     *
     * compacted: seq &lt; 3 (visible from seq 3, inclusive); seq 2 retained as floor for a at seq 3
     *
     *   +------------+----------+--------------------+
     *   | operation  | result   | note               |
     *   +------------+----------+--------------------+
     *   | revision() | 15       | no writes staged   |
     *   +------------+----------+--------------------+
     *   | put(a, v4) |          |                    |
     *   +------------+----------+--------------------+
     *   | revision() | 16       | first write staged |
     *   +------------+----------+--------------------+
     * </pre>
     *
     * @return the writer's current commitSeq, or the last committed commitSeq if nothing staged
     */
    long revision();

    /** @return the underlying backend write handle */
    WriteHandle handle();

    /**
     * Logical commit: staged writes are ready to be read by other readers after this
     * call returns. Has no effect if no writes were staged.
     */
    @Override
    void close();
}
