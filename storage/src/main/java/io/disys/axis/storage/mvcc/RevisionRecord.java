package io.disys.axis.storage.mvcc;

public record RevisionRecord(
        Revision revision,
        Record record
) implements Comparable<Revision> {
    @Override
    public int compareTo(Revision o) {
        return revision.compareTo(o);
    }

}
