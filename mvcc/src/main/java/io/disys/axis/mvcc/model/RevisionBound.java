package io.disys.axis.mvcc.model;

/**
 * An inclusive commit-sequence range {@code [min, max]} used as a filter on
 * {@code modifiedAtSeq} or {@code createdAtSeq} in range and count queries.
 *
 * @param min  inclusive lower bound on {@code commitSeq}
 * @param max  inclusive upper bound on {@code commitSeq}
 */
public record RevisionBound(long min, long max) {

    /** Unbounded sentinel that matches every commit sequence; equivalent to no filter. */
    public static final RevisionBound ALL = new RevisionBound(Long.MIN_VALUE, Long.MAX_VALUE);

    public RevisionBound {
        if (min > max) throw new IllegalArgumentException("min cannot be greater than max");
    }

    /**
     * Returns {@code true} if this bound is unconstrained, allowing the filter
     * to be skipped entirely.
     *
     * @return {@code true} if both ends are at their extreme values
     */
    public boolean isAll() {
        return this == ALL || (min == Long.MIN_VALUE && max == Long.MAX_VALUE);
    }

    /**
     * @param seq  the commit sequence to test
     * @return {@code true} if {@code min <= seq <= max}
     */
    public boolean test(long seq) {
        return min <= seq && seq <= max;
    }
}
