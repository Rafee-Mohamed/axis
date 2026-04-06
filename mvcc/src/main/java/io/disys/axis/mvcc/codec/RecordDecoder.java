package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;

import java.nio.ByteBuffer;

import static io.disys.axis.mvcc.codec.CodecConstants.*;

public class RecordDecoder {

    public RevisionRecord decode(byte[] revision, byte[] record) {
        return new RevisionRecord(decodeRevision(revision), decodeRecord(record));
    }

    public Record decodeRecord(byte[] record) {
        var buffer = ByteBuffer.wrap(record);

        var tombstone = buffer.get();

        var key = new byte[buffer.getInt()];
        buffer.get(key);
        var val = new byte[record.length - RECORD_OVERHEAD - key.length];
        var isTombstone = tombstone == TOMBSTONE;
        if (!isTombstone) {
            buffer.get(val);
        }

        return new Record(key, val, isTombstone, buffer.getInt(), buffer.getLong(), buffer.getLong());
    }

    public Revision decodeRevision(byte[] revision) {
        var buffer = ByteBuffer.wrap(revision);
        return new Revision(buffer.getLong(), buffer.getInt());
    }
}
