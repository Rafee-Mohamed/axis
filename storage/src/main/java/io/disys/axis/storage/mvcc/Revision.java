package io.disys.axis.storage.mvcc;

public record Revision(long commitSeq, int ordinal) implements Comparable<Revision> {
    @Override
    public int compareTo(Revision other) {
        return Long.compare(commitSeq, other.commitSeq);
    }
}
