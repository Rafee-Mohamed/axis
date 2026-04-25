package io.disys.axis.mvcc.model;

import io.disys.axis.mvcc.timeline.KeySpan;

public record Record(
        byte[] key,
        byte[] val,
        boolean tombstone,
        int version,
        long createdAtSeq,
        long modifiedAtSeq
) {
    public Record(byte[] key, byte[] val, KeySpan span) {
        this(key, val, false, span.version(), span.createdAt(), span.modifiedAt());
    }

    public Record(byte[] key, KeySpan span) {
        this(key, new byte[0], true, span.version(), span.createdAt(), span.modifiedAt());
    }
}
