package io.disys.axis.storage.mvcc;

public interface KeySpan {
    long createdAtSeq();
    long modifiedAtSeq();

    Revision lastRevision();
    Revision firstRevision();

    int version();
}
