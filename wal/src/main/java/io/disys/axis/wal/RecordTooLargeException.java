package io.disys.axis.wal;

public class RecordTooLargeException extends Exception {
    public RecordTooLargeException(long payloadSize, long maxRecordSize) {
        super("Record with size " + payloadSize + " exceeds maxRecordSize " + maxRecordSize);
    }
}
