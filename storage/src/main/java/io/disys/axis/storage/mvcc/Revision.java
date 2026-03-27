package io.disys.axis.storage.mvcc;

public record Revision(long commitSeq, int ordinal) implements Comparable<Revision> {
    @Override
    public int compareTo(Revision other) {
        var cmp = compareTo(other.commitSeq());
        return cmp == 0 ? Integer.compare(ordinal, other.ordinal) : cmp;
    }

    public int compareTo(long otherCommitSeq) {
        return Long.compare(commitSeq, otherCommitSeq);
    }
}
