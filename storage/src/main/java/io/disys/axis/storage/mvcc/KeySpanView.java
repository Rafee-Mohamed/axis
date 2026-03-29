package io.disys.axis.storage.mvcc;

public interface KeySpanView {

    long createdAtSeq();
    long modifiedAtSeq();

    Revision lastRevision();
    Revision firstRevision();

    int lastVersion();

}
