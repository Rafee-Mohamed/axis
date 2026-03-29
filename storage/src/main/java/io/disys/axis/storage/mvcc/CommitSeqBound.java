package io.disys.axis.storage.mvcc;

public class CommitSeqBound {
    private volatile long start;
    private volatile long end;

    CommitSeqBound(long start, long end) {
        this.start = start;
        this.end = end;
    }

    long start() {
        return start;
    }

    long end() {
        return end;
    }

    void compact(long commitSeq) {
        start = commitSeq;
    }

    void advance() {
        ++end;
    }

}
