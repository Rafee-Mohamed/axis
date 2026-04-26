package io.disys.axis.mvcc.store;

import io.disys.axis.backend.ReadHandle;
import io.disys.axis.mvcc.model.CountOptions;
import io.disys.axis.mvcc.model.Page;
import io.disys.axis.mvcc.model.RangeOptions;
import io.disys.axis.mvcc.model.Record;

import java.util.Optional;

public interface Reader extends AutoCloseable {
    Optional<Record> get(byte[] key);
    SnapshotResult<Optional<Record>> getAt(byte[] key, long commitSeq);

    Page<Record> range(byte[] from, byte[] to);
    Page<Record> range(byte[] from, byte[] to, RangeOptions options);

    SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Page<Record>> rangeAt(byte[] from, byte[] to, long commitSeq, RangeOptions options);

    Page<byte[]> keys(byte[] from, byte[] to);
    Page<byte[]> keys(byte[] from, byte[] to, RangeOptions options);

    SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Page<byte[]>> keysAt(byte[] from, byte[] to, long commitSeq, RangeOptions options);

    long count(byte[] from, byte[] to);
    long count(byte[] from, byte[] to, CountOptions options);

    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq);
    SnapshotResult<Long> countAt(byte[] from, byte[] to, long commitSeq, CountOptions options);

    long revision();

    ReadHandle handle();
    @Override
    void close();
}
