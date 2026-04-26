package io.disys.axis.mvcc.model;

/**
 * An inclusive version range {@code [min, max]} used as a filter on
 * {@link Record#version()} in range and count queries.
 *
 * @param min  inclusive lower bound on version
 * @param max  inclusive upper bound on version
 */
public record VersionBound(int min, int max) {

    /** Unbounded sentinel that matches every version; equivalent to no filter. */
    public static final VersionBound ALL = new VersionBound(Integer.MIN_VALUE, Integer.MAX_VALUE);

    public VersionBound {
        if (min > max) throw new IllegalArgumentException("min cannot be greater than max");
    }

    /**
     * Returns {@code true} if this bound is unconstrained, allowing the filter
     * to be skipped entirely.
     *
     * @return {@code true} if both ends are at their extreme values
     */
    public boolean isAll() {
        return this == ALL || (min == Integer.MIN_VALUE && max == Integer.MAX_VALUE);
    }

    /**
     * @param version  the version to test
     * @return {@code true} if {@code min <= version <= max}
     */
    public boolean test(int version) {
        return min <= version && version <= max;
    }
}
