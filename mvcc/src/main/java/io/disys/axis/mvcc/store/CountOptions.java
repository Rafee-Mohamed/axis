package io.disys.axis.mvcc.store;

public final class CountOptions {

    private final RevisionBound modifiedIn;
    private final RevisionBound createdIn;

    private CountOptions(RevisionBound modifiedIn, RevisionBound createdIn) {
        this.modifiedIn = modifiedIn;
        this.createdIn = createdIn;
    }

    public RevisionBound modifiedIn()   { return modifiedIn; }
    public RevisionBound createdIn()    { return createdIn; }

    public boolean hasModifiedFilter()  { return !modifiedIn.isAll(); }
    public boolean hasCreatedFilter()   { return !createdIn.isAll(); }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private RevisionBound modifiedIn = RevisionBound.ALL;
        private RevisionBound createdIn  = RevisionBound.ALL;

        public Builder modifiedIn(long min, long max) { this.modifiedIn = new RevisionBound(min, max); return this; }
        public Builder createdIn(long min, long max)  { this.createdIn = new RevisionBound(min, max); return this; }

        public CountOptions build() { return new CountOptions(modifiedIn, createdIn); }
    }
}
