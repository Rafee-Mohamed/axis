package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;

import java.nio.ByteBuffer;

import static io.disys.axis.mvcc.codec.CodecConstants.*;


public class RecordEncoder {

    public byte[] encode(Revision revision) {
        var encodedRevision = ByteBuffer.allocate(REVISION_SIZE);
        encodedRevision.putLong(revision.commitSeq());
        encodedRevision.putInt(revision.ordinal());
        return encodedRevision.array();
    }

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


    public EncodedRevisionRecord encode(RevisionRecord rr) {
        return new EncodedRevisionRecord(
                encode(rr.revision()),
                encode(rr.record())
        );
    }

}
