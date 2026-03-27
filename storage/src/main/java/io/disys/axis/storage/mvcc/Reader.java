package io.disys.axis.storage.mvcc;

import io.disys.axis.storage.backend.CloseableIterator;

import java.io.IOException;
import java.util.Optional;

public interface Reader {
    Optional<Record> get(byte[] key) throws IOException;
    CloseableIterator<KeyVal> range(byte[] key, byte[] val);
}
