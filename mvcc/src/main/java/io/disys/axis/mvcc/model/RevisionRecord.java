package io.disys.axis.mvcc.model;

public record RevisionRecord(
        Revision revision,
        Record record
) implements Comparable<Revision> {
    @Override
    public int compareTo(Revision o) {
        return revision.compareTo(o);
    }

    public int compareTo(long commitSeq) {
        return revision.compareTo(commitSeq);
    }

}
