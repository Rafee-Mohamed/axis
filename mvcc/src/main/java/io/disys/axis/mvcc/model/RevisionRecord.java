package io.disys.axis.mvcc.model;

/** A {@link Revision} paired with its {@link Record}. */
public record RevisionRecord(
        Revision revision,
        Record record
) implements Comparable<Revision> {
    @Override
    public int compareTo(Revision o) {
        return revision.compareTo(o);
    }

    /**
     * Compares against {@code Revision(commitSeq, 0)}.
     *
     * @param commitSeq  the commit sequence to compare against
     * @return per {@link Comparable} contract
     */
    public int compareTo(long commitSeq) {
        return revision.compareTo(commitSeq);
    }

}
