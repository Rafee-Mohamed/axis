package io.disys.axis.mvcc.model;

public record RevisionBound(long min, long max) {

    public static final RevisionBound ALL = new RevisionBound(Long.MIN_VALUE, Long.MAX_VALUE);

    public RevisionBound {
        if (min > max) throw new IllegalArgumentException("min cannot be greater than max");
    }

    public boolean isAll()          {
        return this == ALL || (min == Long.MIN_VALUE && max == Long.MAX_VALUE);
    }

    public boolean test(long seq)   {
        return min <= seq && seq <= max;
    }
}
