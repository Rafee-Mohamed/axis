package io.disys.axis.storage.mvcc;

public record RevisionRecord(
        Revision revision,
        Record record
) {
}
