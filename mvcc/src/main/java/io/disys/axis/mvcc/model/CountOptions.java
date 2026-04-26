package io.disys.axis.mvcc.model;

/**
 * Filter configuration for count queries.
 *
 * <p>A subset of {@link RangeOptions} without limit or sort — count operations
 * are not ordered and not paginated. Use {@link #builder()} to construct instances.</p>
 */
public final class CountOptions {

    private final RevisionBound modifiedIn;
    private final RevisionBound createdIn;
    private final VersionBound versionIn;

    private CountOptions(RevisionBound modifiedIn, RevisionBound createdIn, VersionBound versionIn) {
        this.modifiedIn = modifiedIn;
        this.createdIn = createdIn;
        this.versionIn = versionIn;
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private RevisionBound modifiedIn = RevisionBound.ALL;
        private RevisionBound createdIn = RevisionBound.ALL;
        private VersionBound versionIn = VersionBound.ALL;

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

        public CountOptions build() {
            return new CountOptions(modifiedIn, createdIn, versionIn);
        }
    }
}
