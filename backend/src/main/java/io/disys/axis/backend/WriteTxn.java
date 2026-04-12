package io.disys.axis.backend;

public interface WriteTxn extends WriteHandle, AutoCloseable {
    void commit();
    @Override
    void close();
}
