package io.disys.axis.mvcc.model;

public record VersionBound(int min, int max) {

    public static final VersionBound ALL = new VersionBound(Integer.MIN_VALUE, Integer.MAX_VALUE);

    public VersionBound {
        if (min > max) throw new IllegalArgumentException("min cannot be greater than max");
    }

    public boolean isAll() {
        return this == ALL || (min == Integer.MIN_VALUE && max == Integer.MAX_VALUE);
    }

    public boolean test(int version) {
        return min <= version && version <= max;
    }
}
