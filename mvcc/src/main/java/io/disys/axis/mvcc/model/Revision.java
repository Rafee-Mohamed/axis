package io.disys.axis.mvcc.model;

public record Revision(long commitSeq, int ordinal) implements Comparable<Revision> {

    public static Revision create(long commitSeq) {
        return new Revision(commitSeq, 0);
    }

    public static Revision modify(long commitSeq, int ordinal) {
        return new Revision(commitSeq, ordinal);
    }

    public Revision next() {
        return new Revision(commitSeq, ordinal + 1);
    }

    @Override
    public int compareTo(Revision other) {
        var cmp = Long.compare(commitSeq, other.commitSeq());
        return cmp == 0 ? Integer.compare(ordinal, other.ordinal) : cmp;
    }

    public int compareTo(long otherCommitSeq) {
        var cmp = Long.compare(commitSeq, otherCommitSeq);
        return cmp == 0 ? Integer.compare(ordinal, 0) : cmp;
    }
}
