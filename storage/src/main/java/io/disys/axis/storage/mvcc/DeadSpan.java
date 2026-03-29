package io.disys.axis.storage.mvcc;

import java.util.List;

public record DeadSpan(List<Revision> revisions) implements KeySpanView {

    static DeadSpan create(List<Revision> revisions) {
        if (revisions.size() < 2) {
            throw new IllegalStateException("Dead span should have at lease 2 revisions - create and delete");
        }
        return new DeadSpan(revisions);
    }

    @Override
    public long createdAtSeq() {
        return revisions.getFirst().commitSeq();
    }

    @Override
    public long modifiedAtSeq() {
        return revisions.getLast().commitSeq();
    }

    @Override
    public Revision lastRevision() {
        return revisions.getLast();
    }

    @Override
    public Revision firstRevision() {
        return revisions.getFirst();
    }

    @Override
    public int lastVersion() {
        return revisions.size() - 1;
    }
}
