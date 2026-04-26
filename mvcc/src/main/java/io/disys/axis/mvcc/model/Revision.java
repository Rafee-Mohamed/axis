package io.disys.axis.mvcc.model;

/**
 * A unique, totally-ordered identifier for a single mutation in the store.
 *
 * <p>Every write is assigned a {@code Revision} at the time it is staged.
 * All mutations within a single commit share the same {@code commitSeq} and
 * are distinguished by {@code ordinal} within that commit.</p>
 *
 * <h2>Total Order</h2>
 * <p>Revisions are ordered lexicographically: {@code commitSeq} first, then
 * {@code ordinal}. A greater revision always reflects a strictly later mutation.</p>
 *
 * @param commitSeq  monotonically increasing identifier for the commit that
 *                   produced this mutation; all mutations in a single commit
 *                   carry the same value
 * @param ordinal    zero-based position of this mutation within its commit;
 *                   the first mutation in each commit has ordinal {@code 0}
 */
public record Revision(long commitSeq, int ordinal) implements Comparable<Revision> {

    /**
     * Returns the next revision within the same commit: same {@code commitSeq},
     * {@code ordinal + 1}.
     *
     * @return a new {@code Revision} with the same {@code commitSeq} and incremented {@code ordinal}
     */
    public Revision next() {
        return new Revision(commitSeq, ordinal + 1);
    }

    @Override
    public int compareTo(Revision other) {
        var cmp = Long.compare(commitSeq, other.commitSeq());
        return cmp == 0 ? Integer.compare(ordinal, other.ordinal) : cmp;
    }

    /**
     * Compares against {@code Revision(otherCommitSeq, 0)}.
     *
     * @param otherCommitSeq  the commit sequence to compare against
     * @return per {@link Comparable} contract
     */
    public int compareTo(long otherCommitSeq) {
        var cmp = Long.compare(commitSeq, otherCommitSeq);
        return cmp == 0 ? Integer.compare(ordinal, 0) : cmp;
    }
}
