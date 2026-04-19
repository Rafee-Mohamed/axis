package io.disys.axis.mvcc.store;

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

    public long limit() {
        return limit;
    }

    public RevisionBound modifiedIn() {
        return modifiedIn;
    }

    public RevisionBound createdIn() {
        return createdIn;
    }

    public VersionBound versionIn() {
        return versionIn;
    }

    public SortTarget sortTarget() {
        return sortTarget;
    }

    public SortDirection sortDirection() {
        return sortDirection;
    }

    public boolean hasModifiedFilter() {
        return !modifiedIn.isAll();
    }

    public boolean hasCreatedFilter() {
        return !createdIn.isAll();
    }

    public boolean hasVersionFilter() {
        return !versionIn.isAll();
    }

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
