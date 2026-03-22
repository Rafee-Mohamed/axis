package io.disys.axis.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class Wal {
    private final SegmentManager manager;
    private final WalConfig config;

    Wal(SegmentManager manager, WalConfig config) {
        this.manager = manager;
        this.config = config;
    }

    public static Recovery recover(WalConfig config) throws IOException {
        return Recovery.from(config);
    }

    static Wal open(SegmentManager manager, WalConfig config) {
        return new Wal(manager, config);
    }

    public void append(ByteBuffer payload) throws IOException, RecordTooLargeException, WalClosedException {
        validateRecordSize(payload);
        throwIfClosed();
        appendValid(payload);
    }

    public void append(List<ByteBuffer> payload) throws IOException, RecordTooLargeException, WalClosedException {
        validateRecordSize(payload);
        throwIfClosed();
        appendValid(payload);
    }

    public void appendValid(ByteBuffer payload) throws IOException {
        manager.append(payload);
    }

    public void appendValid(List<ByteBuffer> payload) throws IOException {
        manager.append(payload);
    }

    public void validateRecordSize(List<ByteBuffer> payload) throws RecordTooLargeException {
        for (var buf: payload)
            validateRecordSize(buf);
    }

    public void validateRecordSize(ByteBuffer payload) throws RecordTooLargeException {
        if (payload.remaining() > config.maxRecordSize()) {
            throw new RecordTooLargeException(payload.remaining(), config.maxRecordSize());
        }
    }

    public void throwIfClosed() throws WalClosedException {
        if (!isOpen()) {
            throw new WalClosedException();
        }
    }

    public void truncatePrefix(long index) throws IOException {
        manager.truncatePrefix(index);
    }

    public boolean isOpen() {
        return manager.isOpen();
    }

    public void close() throws IOException {
        manager.close();
    }
}
