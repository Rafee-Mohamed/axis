package io.disys.axis.mvcc.model;

/**
 * Configuration for a range query: result limit, sort order, and value filters.
 *
 * <p>Defaults: no limit ({@link #UNLIMITED}), sort by {@link SortTarget#KEY}
 * {@link SortDirection#ASCENDING}, all filter bounds unconstrained.</p>
 *
 * <p>Use {@link #builder()} to construct instances.</p>
 */
public final class RangeOptions {

    public static final long UNLIMITED = Long.MAX_VALUE;

    private final long limit;
    private final RevisionBound modifiedIn;
    private final RevisionBound createdIn;
    private final VersionBound versionIn;
    private final SortTarget sortTarget;
    private final SortDirection sortDirection;

    private RangeOptions(long limit, RevisionBound modifiedIn, RevisionBound createdIn, VersionBound versionIn, SortTarget sortTarget, SortDirection sortDirection) {
        this.limit = limit;
        this.modifiedIn = modifiedIn;
        this.createdIn = createdIn;
        this.versionIn = versionIn;
        this.sortTarget = sortTarget;
        this.sortDirection = sortDirection;
    }

    /** @return maximum number of results to return; {@link #UNLIMITED} means no limit */
    public long limit() {
        return limit;
    }

    /** @return inclusive {@code commitSeq} range filter on {@code modifiedAtSeq} */
    public RevisionBound modifiedIn() {
        return modifiedIn;
    }

    /** @return inclusive {@code commitSeq} range filter on {@code createdAtSeq} */
    public RevisionBound createdIn() {
        return createdIn;
    }

    /** @return inclusive version range filter on {@link Record#version()} */
    public VersionBound versionIn() {
        return versionIn;
    }

    /** @return the field by which results are sorted */
    public SortTarget sortTarget() {
        return sortTarget;
    }

    /** @return the direction in which results are sorted */
    public SortDirection sortDirection() {
        return sortDirection;
    }

    /** @return {@code true} if {@code modifiedIn} is not the unbounded default */
    public boolean hasModifiedFilter() {
        return !modifiedIn.isAll();
    }

    /** @return {@code true} if {@code createdIn} is not the unbounded default */
    public boolean hasCreatedFilter() {
        return !createdIn.isAll();
    }

    /** @return {@code true} if {@code versionIn} is not the unbounded default */
    public boolean hasVersionFilter() {
        return !versionIn.isAll();
    }

    /** @return {@code true} if results are sorted by {@link SortTarget#KEY} */
    public boolean isKeySort() {
        return sortTarget == SortTarget.KEY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long limit = UNLIMITED;
        private RevisionBound modifiedIn = RevisionBound.ALL;
        private RevisionBound createdIn = RevisionBound.ALL;
        private VersionBound versionIn = VersionBound.ALL;
        private SortTarget sortTarget = SortTarget.KEY;
        private SortDirection sortDirection = SortDirection.ASCENDING;

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder modifiedIn(long min, long max) {
            this.modifiedIn = new RevisionBound(min, max);
            return this;
        }

        public Builder createdIn(long min, long max) {
            this.createdIn = new RevisionBound(min, max);
            return this;
        }

        public Builder versionIn(int min, int max) {
            this.versionIn = new VersionBound(min, max);
            return this;
        }

        public Builder sortTarget(SortTarget target) {
            this.sortTarget = target;
            return this;
        }

        public Builder sortDirection(SortDirection dir) {
            this.sortDirection = dir;
            return this;
        }

        public RangeOptions build() {
            return new RangeOptions(limit, modifiedIn, createdIn, versionIn, sortTarget, sortDirection);
        }
    }
}
