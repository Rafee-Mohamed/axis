package io.disys.axis.backend;

public interface ReadTxn extends ReadHandle, AutoCloseable {
    @Override
    void close();
}
