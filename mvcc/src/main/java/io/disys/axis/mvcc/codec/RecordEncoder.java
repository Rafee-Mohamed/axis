package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;

import java.nio.ByteBuffer;

import static io.disys.axis.mvcc.codec.CodecConstants.*;

/** Encodes {@link Revision} and {@link Record} instances to their binary representations. */
public class RecordEncoder {

    /**
     * @param revision  the revision to encode
     * @return the encoded revision bytes per the layout in {@link CodecConstants}
     */
    public byte[] encode(Revision revision) {
        var encodedRevision = ByteBuffer.allocate(REVISION_SIZE);
        encodedRevision.putLong(revision.commitSeq());
        encodedRevision.putInt(revision.ordinal());
        return encodedRevision.array();
    }

    /**
     * @param record  the record to encode
     * @return the encoded record bytes per the layout in {@link CodecConstants}
     */
    public byte[] encode(Record record) {
        var key = record.key();
        var val = record.val();

        var keyValSize = KEY_LEN_SIZE + key.length + val.length;
        var encodedRecord = ByteBuffer.allocate(TOMBSTONE_MARKER_SIZE + keyValSize + METADATA_SIZE);

        encodedRecord.put(record.tombstone() ? TOMBSTONE : NO_TOMBSTONE);

        encodedRecord.putInt(key.length);
        encodedRecord.put(key);
        encodedRecord.put(val);

        encodedRecord.putInt(record.version());
        encodedRecord.putLong(record.createdAtSeq());
        encodedRecord.putLong(record.modifiedAtSeq());

        return encodedRecord.array();
    }

    /**
     * @param rr  the revision record to encode
     * @return an {@link EncodedRevisionRecord} containing the encoded revision and record
     */
    public EncodedRevisionRecord encode(RevisionRecord rr) {
        return new EncodedRevisionRecord(
                encode(rr.revision()),
                encode(rr.record())
        );
    }

}
