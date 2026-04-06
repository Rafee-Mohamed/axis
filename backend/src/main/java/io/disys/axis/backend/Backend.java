package io.disys.axis.backend;

public interface Backend extends AutoCloseable {
    WriteTxn beginWrite();
    ReadTxn beginRead();
    Snapshot snapshot();
}
