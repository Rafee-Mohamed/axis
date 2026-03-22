package io.disys.axis.storage.backend;

public interface Backend extends AutoCloseable {
    WriteTx beginWrite();
    ReadTx beginRead();
    Snapshot snapshot();
}
