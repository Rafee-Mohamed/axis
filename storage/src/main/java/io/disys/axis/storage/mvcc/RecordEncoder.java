package io.disys.axis.storage.mvcc;

import java.nio.ByteBuffer;

import static io.disys.axis.storage.mvcc.CodecConstants.*;


public class RecordEncoder {

    byte[] encodeRevision(Revision revision) {
        var encodedRevision = ByteBuffer.wrap(new byte[REVISION_SIZE]);
        encodedRevision.putLong(revision.commitSeq());
        encodedRevision.putInt(revision.ordinal());
        return encodedRevision.array();
    }

    byte[] encodeRecord(byte[] key, byte[] val, KeySpan span) {
        var keyValSize = Long.BYTES + key.length + val.length;
        var encodedRecord = ByteBuffer.wrap(new byte[TOMBSTONE_MARKER_SIZE + keyValSize + METADATA_SIZE]);

        encodedRecord.put(NO_TOMBSTONE);

        encodedRecord.putInt(key.length);
        encodedRecord.put(key);
        encodedRecord.put(val);

        encodedRecord.putInt(span.version());
        encodedRecord.putLong(span.createdAtSeq());
        encodedRecord.putLong(span.modifiedAtSeq());

        return encodedRecord.array();
    }

    byte[] encodeRecord(byte[] key, KeySpan span) {
        var keySize = KEY_LEN_SIZE + key.length;
        var encodedRecord = ByteBuffer.wrap(new byte[TOMBSTONE_MARKER_SIZE + keySize + METADATA_SIZE]);

        encodedRecord.put(TOMBSTONE);

        encodedRecord.putLong(key.length);
        encodedRecord.put(key);

        encodedRecord.putInt(span.version());
        encodedRecord.putLong(span.createdAtSeq());
        encodedRecord.putLong(span.modifiedAtSeq());

        return encodedRecord.array();
    }

    EncodedRevisionRecord encode(Revision revision, byte[] key, byte[] val, KeySpan span) {
        return new EncodedRevisionRecord(
                encodeRevision(revision),
                encodeRecord(key, val, span)
        );
    }

    EncodedRevisionRecord encode(Revision revision, byte[] key, KeySpan span) {
        return new EncodedRevisionRecord(
                encodeRevision(revision),
                encodeRecord(key, span)
        );
    }

}
