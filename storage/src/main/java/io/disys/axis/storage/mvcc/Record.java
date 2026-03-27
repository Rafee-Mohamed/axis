package io.disys.axis.storage.mvcc;

public record Record(
        byte[] key,
        byte[] val,
        int version,
        long createdAtSeq,
        long modifiedAtSeq
) {
    Record(byte[] key, byte[] val, KeySpan span) {
        this(key, val, span.version(), span.createdAtSeq(), span.modifiedAtSeq());
    }

    Record(byte[] key, KeySpan span) {
        this(key, new byte[0], span.version(), span.createdAtSeq(), span.modifiedAtSeq());
    }

    boolean isTombstone() {
        return val.length == 0;
    }
}
