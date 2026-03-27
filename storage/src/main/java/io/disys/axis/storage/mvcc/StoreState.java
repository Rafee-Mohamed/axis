package io.disys.axis.storage.mvcc;

public class StoreState {
    private volatile long lastVisibleCommitSeq;
    private volatile long firstVisibleCommitSeq;

    StoreState(long lastVisibleCommitSeq, long firstVisibleCommitSeq) {
        this.lastVisibleCommitSeq = lastVisibleCommitSeq;
        this.firstVisibleCommitSeq = firstVisibleCommitSeq;
    }

    long lastVisibleCommitSeq() {
        return lastVisibleCommitSeq;
    }

    long firstVisibleCommitSeq() {
        return firstVisibleCommitSeq;
    }

    void compactCommitSeq(long commitSeq) {
        firstVisibleCommitSeq = commitSeq;
    }

    void advanceCommitSeq() {
        ++lastVisibleCommitSeq;
    }

}
