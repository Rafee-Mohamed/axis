package io.disys.axis.mvcc.model;

public final class CountOptions {

    private final RevisionBound modifiedIn;
    private final RevisionBound createdIn;
    private final VersionBound versionIn;

    private CountOptions(RevisionBound modifiedIn, RevisionBound createdIn, VersionBound versionIn) {
        this.modifiedIn = modifiedIn;
        this.createdIn = createdIn;
        this.versionIn = versionIn;
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

    public boolean hasModifiedFilter() {
        return !modifiedIn.isAll();
    }

    public boolean hasCreatedFilter() {
        return !createdIn.isAll();
    }

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
