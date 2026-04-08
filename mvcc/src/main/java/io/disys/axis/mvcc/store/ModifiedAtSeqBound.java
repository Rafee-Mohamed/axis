package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.Revision;

import java.util.function.Predicate;

public record ModifiedAtSeqBound(long min, long max) implements Predicate<Revision> {

    public static ModifiedAtSeqBound bound(long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("min can't be greater than max for bound");
        }
        return new ModifiedAtSeqBound(min, max);
    }

    public static ModifiedAtSeqBound to(long max) {
        return new ModifiedAtSeqBound(Long.MIN_VALUE, max);
    }

    public static ModifiedAtSeqBound from(long min) {
        return new ModifiedAtSeqBound(min, Long.MAX_VALUE);
    }

    @Override
    public boolean test(Revision revision) {
        return min <= revision.commitSeq() && max >= revision.commitSeq();
    }
}
