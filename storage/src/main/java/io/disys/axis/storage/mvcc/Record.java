package io.disys.axis.storage.mvcc;

public record Record(
        byte[] key,
        byte[] val,
        boolean tombstone,
        int version,
        long createdAtSeq,
        long modifiedAtSeq
) {
    Record(byte[] key, byte[] val, KeySpan span) {
        this(key, val, false, span.lastVersion(), span.createdAtSeq(), span.modifiedAtSeq());
    }

    Record(byte[] key, KeySpan span) {
        this(key, new byte[0], true, span.lastVersion(), span.createdAtSeq(), span.modifiedAtSeq());
    }
}
