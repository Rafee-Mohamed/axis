package io.disys.axis.mvcc.store;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.mvcc.model.CountOptions;
import io.disys.axis.mvcc.model.Page;
import io.disys.axis.mvcc.model.RangeOptions;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.model.SnapshotResult;
import io.disys.axis.mvcc.model.SortDirection;
import io.disys.axis.mvcc.model.SortTarget;

import java.util.Optional;

/**
 * A read-only view over the MVCC store, fixed to a commitSeq at creation time.
 * Provides two families of operations: current reads at the reader's fixed commitSeq,
 * and time-travel reads at an explicit commitSeq returning {@link SnapshotResult} to
 * signal when the requested point falls outside the visible window.
 */
public interface Reader extends AutoCloseable {

    /**
     * Returns the record for {@code key} at the reader's current commitSeq. The reader is fixed
     * at the latest committed seq at creation time; by the time queries run, the store may have
     * moved forward but those commits are not visible to this reader.
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
     * </pre>
     *
     * <pre>
     *   +----------+--------+-------------------+------------------------------------------+
     *   | fixed at | query  | result            | note                                     |
     *   +----------+--------+-------------------+------------------------------------------+
     *   |        5 | get(a) | of(Record(a, v1)) | a(v1) from floor (seq 2)                 |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(b) | of(Record(b, v1)) |                                          |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(c) | empty()           | not yet created                          |
     *   +----------+--------+-------------------+------------------------------------------+
     *   |       11 | get(a) | empty()           | deleted at 10                            |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(b) | of(Record(b, v1)) |                                          |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(c) | of(Record(c, v1)) | created at seq 8                         |
     *   +----------+--------+-------------------+------------------------------------------+
     *   |       15 | get(a) | of(Record(a, v3)) | recreated at seq 15                      |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(b) | empty()           | deleted at 14                            |
     *   |          +--------+-------------------+------------------------------------------+
     *   |          | get(c) | of(Record(c, v1)) |                                          |
     *   +----------+--------+-------------------+------------------------------------------+
     * </pre>
     *
     * @param key the key to look up
     * @return the record visible at the reader's current commitSeq, or empty if absent or deleted
     */
    Optional<Record> get(byte[] key);

    /**
     * Returns the record for {@code key} at an explicit {@code commitSeq}. The reader is fixed
     * at the latest committed seq at creation time; querying any seq beyond that returns Future,
     * regardless of whether the store has since moved forward.
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
     * </pre>
     *
     * <pre>
     *   +----------+--------------+------------------------+-----------------------------------------+
     *   | fixed at | query        | result                 | note                                    |
     *   +----------+--------------+------------------------+-----------------------------------------+
     *   |       15 | getAt(a,  3) | Ok(of(Record(a, v1)))  | a(v1) from floor (seq 2)                |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(b,  4) | Ok(of(Record(b, v1)))  |                                         |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a,  7) | Ok(of(Record(a, v2)))  | a updated at seq 6                      |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(c,  8) | Ok(of(Record(c, v1)))  |                                         |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a, 10) | Ok(empty())            | deleted at seq 10                       |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a, 12) | Ok(empty())            | in [10, 15) - deleted, not yet recreated|
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(b, 14) | Ok(empty())            | b deleted at seq 14                     |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a, 15) | Ok(of(Record(a, v3)))  | recreated at seq 15                     |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a,  1) | Compacted(3, 1)        | below compaction boundary               |
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a,  2) | Compacted(3, 2)        | seq 2 exists as floor but below boundary|
     *   |          +--------------+------------------------+-----------------------------------------+
     *   |          | getAt(a, 20) | Future(15, 20)         | seq 20 beyond last committed in store   |
     *   +----------+--------------+------------------------+-----------------------------------------+
     *   |        8 | getAt(a, 10) | Future(8, 10)          | seq 10 beyond reader's fixed seq        |
     *   +----------+--------------+------------------------+-----------------------------------------+
     * </pre>
     *
     * @param key       the key to look up
     * @param commitSeq the point in time to query
     * @return Ok with the record, Compacted if below the compaction boundary, Future if beyond the current revision
     */
    SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq);

    /**
     * Scans records in the half-open interval {@code [from, to)} at the reader's current commitSeq.
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
     * </pre>
     *
     * <pre>
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   | fixed at | query       | result                | note                                          |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |        5 | range(a, d) | [a(v1), b(v1)]        | a(v1) from floor (seq 2); c not yet present   |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |        7 | range(a, d) | [a(v2), b(v1)]        | a updated at seq 6                            |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |        9 | range(a, d) | [a(v2), b(v1), c(v1)] |                                               |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |       11 | range(a, d) | [b(v1), c(v1)]        | a deleted at 10                               |
     *   |          +-------------+-----------------------+-----------------------------------------------+
     *   |          | range(a, b) | []                    | only a in range, deleted at 10                |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |       13 | range(a, d) | [b(v2), c(v1)]        | b updated at 12                               |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     *   |       15 | range(a, d) | [a(v3), c(v1)]        | a recreated at 15, b deleted at 14            |
     *   +----------+-------------+-----------------------+-----------------------------------------------+
     * </pre>
     *
     * @param from the inclusive lower bound
     * @param to   the exclusive upper bound
     * @return a complete page of records visible at the current commitSeq; {@code more} is always {@code false}
     */
    Page<Record> range(byte[] from, byte[] to);

    /**
     * Same as {@link #range(byte[], byte[])}; options add a result limit, sort order
     * (per {@link SortTarget} and {@link SortDirection}), and filters on creation seq,
     * modification seq, or record version.
     *
     * @param from    the inclusive lower bound
     * @param to      the exclusive upper bound
     * @param options limit, sort, and filter options
     * @return a complete page of records visible at the current commitSeq, filtered and sorted per options
     */
    Page<Record> range(byte[] from, byte[] to, RangeOptions options);

    /**
     * Scans records in {@code [from, to)} at an explicit {@code commitSeq}.
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
     * </pre>
     *
     * <pre>
     *   +----------+--------------------+----------------------+---------------------------------------+
     *   | fixed at | query              | result               | note                                  |
     *   +----------+--------------------+----------------------+---------------------------------------+
     *   |       15 | rangeAt(a, d,  5)  | Ok([a(v1), b(v1)])   | a(v1) from floor (seq 2)              |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d,  7)  | Ok([a(v2), b(v1)])   | a updated at seq 6                    |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d, 10)  | Ok([b(v1), c(v1)])   | a deleted at 10                       |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d, 13)  | Ok([b(v2), c(v1)])   | b updated at 12                       |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d, 14)  | Ok([c(v1)])          | b deleted at 14                       |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, c, 14)  | Ok([])               | a and b deleted, c out of range       |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d, 15)  | Ok([a(v3), c(v1)])   | a recreated, b still deleted          |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d,  1)  | Compacted(3, 1)      | below compaction boundary             |
     *   |          +--------------------+----------------------+---------------------------------------+
     *   |          | rangeAt(a, d, 20)  | Future(15, 20)       | seq 20 beyond last committed in store |
     *   +----------+--------------------+----------------------+---------------------------------------+
     *   |       10 | rangeAt(a, d, 12)  | Future(10, 12)       | seq 12 beyond reader's fixed seq      |
     *   +----------+--------------------+----------------------+---------------------------------------+
     * </pre>
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @return Ok with a complete page, Compacted if below the compaction boundary, Future if beyond current revision; {@code more} is always {@code false}
     */
    SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq);

    /**
     * Same as {@link #rangeAt(byte[], byte[], long)}; options add a result limit, sort order (per {@link SortTarget} and {@link SortDirection}),
     * and filters on creation seq, modification seq, or record version.
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @param options   limit, sort, and filter options
     * @return Ok with a complete page, Compacted if below the compaction boundary, Future if beyond current revision
     */
    SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options);

    /**
     * Same as {@link #range(byte[], byte[])} mapped to keys only; each record in the result
     * is reduced to its key.
     *
     * @param from the inclusive lower bound
     * @param to   the exclusive upper bound
     * @return a complete page of keys visible at the current commitSeq; {@code more} is always {@code false}
     */
    Page<byte[]> keys(byte[] from, byte[] to);

    /**
     * Same as {@link #keys(byte[], byte[])}; options add a result limit, sort order (per {@link SortTarget} and {@link SortDirection}),
     * and filters on creation seq, modification seq, or record version.
     *
     * @param from    the inclusive lower bound
     * @param to      the exclusive upper bound
     * @param options limit, sort, and filter options
     * @return a complete page of keys visible at the current commitSeq, filtered and sorted per options
     */
    Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options);

    /**
     * Same as {@link #rangeAt(byte[], byte[], long)} mapped to keys only.
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @return Ok with a complete page of keys, Compacted if below boundary, Future if beyond current revision
     */
    SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq);

    /**
     * Same as {@link #keysAt(byte[], byte[], long)}; options add a result limit, sort order (per {@link SortTarget} and {@link SortDirection}),
     * and filters on creation seq, modification seq, or record version.
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @param options   limit, sort, and filter options
     * @return Ok with a complete page of keys, Compacted if below boundary, Future if beyond current revision
     */
    SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options);

    /**
     * Counts keys in {@code [from, to)} at the reader's current commitSeq.
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
     * </pre>
     *
     * <pre>
     *   +----------+-------------+--------+------------------------------+
     *   | fixed at | query       | result | note                         |
     *   +----------+-------------+--------+------------------------------+
     *   |        5 | count(a, d) | 2      | a(v1) from floor; c not yet  |
     *   +----------+-------------+--------+------------------------------+
     *   |        9 | count(a, d) | 3      |                              |
     *   +----------+-------------+--------+------------------------------+
     *   |       11 | count(a, d) | 2      | a deleted at 10              |
     *   |          +-------------+--------+------------------------------+
     *   |          | count(a, b) | 0      | only a in range, deleted     |
     *   +----------+-------------+--------+------------------------------+
     *   |       15 | count(a, d) | 2      | b deleted at 14              |
     *   +----------+-------------+--------+------------------------------+
     * </pre>
     *
     * @param from the inclusive lower bound
     * @param to   the exclusive upper bound
     * @return the count of keys visible at the current commitSeq
     */
    long count(byte[] from, byte[] to);

    /**
     * Same as {@link #count(byte[], byte[])}; options filter by creation seq, modification seq,
     * or record version. Count has no limit or sort.
     *
     * @param from    the inclusive lower bound
     * @param to      the exclusive upper bound
     * @param options filter options
     * @return the count of keys visible at the current commitSeq, filtered per options
     */
    long count(byte[] from, byte[] to, CountOptions options);

    /**
     * Counts keys in {@code [from, to)} at an explicit {@code commitSeq}.
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
     * </pre>
     *
     * <pre>
     *   +----------+--------------------+-----------------+---------------------------------------+
     *   | fixed at | query              | result          | note                                  |
     *   +----------+--------------------+-----------------+---------------------------------------+
     *   |       15 | countAt(a, d,  5)  | Ok(2)           | a(v1) from floor; c not yet           |
     *   |          +--------------------+-----------------+---------------------------------------+
     *   |          | countAt(a, d,  9)  | Ok(3)           |                                       |
     *   |          +--------------------+-----------------+---------------------------------------+
     *   |          | countAt(a, d, 11)  | Ok(2)           | a deleted at 10                       |
     *   |          +--------------------+-----------------+---------------------------------------+
     *   |          | countAt(a, d, 15)  | Ok(2)           | a(v3), c(v1); b deleted at 14         |
     *   |          +--------------------+-----------------+---------------------------------------+
     *   |          | countAt(a, d,  1)  | Compacted(3, 1) | below compaction boundary             |
     *   |          +--------------------+-----------------+---------------------------------------+
     *   |          | countAt(a, d, 20)  | Future(15, 20)  | seq 20 beyond last committed in store |
     *   +----------+--------------------+-----------------+---------------------------------------+
     *   |        8 | countAt(a, d, 12)  | Future(8, 12)   | seq 12 beyond reader's fixed seq      |
     *   +----------+--------------------+-----------------+---------------------------------------+
     * </pre>
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @return Ok with the count, Compacted if below boundary, Future if beyond current revision
     */
    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq);

    /**
     * Same as {@link #countAt(byte[], byte[], long)}; options filter by creation seq,
     * modification seq, or record version. Count has no limit or sort.
     *
     * @param from      the inclusive lower bound
     * @param to        the exclusive upper bound
     * @param commitSeq the point in time to query
     * @param options   filter options
     * @return Ok with the count, Compacted if below boundary, Future if beyond current revision
     */
    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options);

    /** @return the commitSeq this reader is fixed at */
    long revision();

    /** @return the underlying backend read handle */
    ReadHandle handle();

    /** Releases any resources held by this reader. */
    @Override
    void close();
}
