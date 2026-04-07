package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.timeline.*;

import io.disys.axis.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Reader extends AutoCloseable {
    ReadResult get(byte[] key);
    ReadResult getAt(byte[] key, long commitSeq);
    CloseableIterator<io.disys.axis.mvcc.model.Record> range(byte[] startKey, byte[] endKey);
    CloseableIterator<io.disys.axis.mvcc.model.Record> rangeAt(byte[] startKey, byte[] endKey, long commitSeq);
    @Override
    void close();
}
