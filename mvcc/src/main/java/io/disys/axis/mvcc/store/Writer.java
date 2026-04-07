package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Writer extends Reader, AutoCloseable {
    void put(byte[] key, byte[] val);
    boolean delete(byte[] key);
    @Override
    void close();
}
