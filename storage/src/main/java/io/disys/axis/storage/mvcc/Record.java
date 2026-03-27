package io.disys.axis.storage.mvcc;

public record Record(
        byte[] key,
        byte[] val,
        int version,
        long createdAtSeq,
        long modifiedAtSeq
) {
}
