package io.disys.axis.storage.backend;

public interface Backend extends AutoCloseable {
    WriteTxn beginWrite();
    ReadTxn beginRead();
    Snapshot snapshot();
}
