package io.disys.axis.storage.mvcc;

import java.nio.ByteBuffer;

import static io.disys.axis.storage.mvcc.CodecConstants.*;


public class RecordEncoder {

    byte[] encode(Revision revision) {
        var encodedRevision = ByteBuffer.allocate(REVISION_SIZE);
        encodedRevision.putLong(revision.commitSeq());
        encodedRevision.putInt(revision.ordinal());
        return encodedRevision.array();
    }

    byte[] encode(Record record) {
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


    EncodedRevisionRecord encode(RevisionRecord rr) {
        return new EncodedRevisionRecord(
                encode(rr.revision()),
                encode(rr.record())
        );
    }

}
