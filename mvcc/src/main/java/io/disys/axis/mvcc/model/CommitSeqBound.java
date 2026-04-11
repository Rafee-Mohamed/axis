package io.disys.axis.mvcc.model;

public class CommitSeqBound {
    private long start;
    private volatile long end;

    public CommitSeqBound(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    public long next() {
        return end + 1;
    }

    public void compact(long commitSeq) {
        start = commitSeq;
    }

    public void advance() {
        ++end;
    }

}
