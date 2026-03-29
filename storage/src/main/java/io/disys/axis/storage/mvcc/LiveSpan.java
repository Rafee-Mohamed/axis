package io.disys.axis.storage.mvcc;

public record LiveSpan(int position, VolatileList<Revision> revisions) implements KeySpanView {

    // create and update revision
    void add(Revision revision) {
        revisions.add(revision);
    }

    @Override
    public long createdAtSeq() {
        return revisions.getFirst().commitSeq();
    }

    @Override
    public long modifiedAtSeq() {
        return revisions.getLast().commitSeq();
    }

    boolean isEmpty() {
        return revisions.isEmpty();
    }

    @Override
    public Revision firstRevision() {
        return revisions.getFirst();
    }

    @Override
    public Revision lastRevision() {
        return revisions.getLast();
    }

    @Override
    public int lastVersion() {
        return revisions.size() - 1;
    }

    Revision get(int idx) {
        return revisions.get(idx);
    }

    int size() { return revisions.size(); }

    DeadSpan complete(Revision deleteRevision) {
        var nextDeadSpan = revisions.toList();
        nextDeadSpan.add(deleteRevision);
        return DeadSpan.create(nextDeadSpan);
    }

    void release() {
        revisions.release();
    }

    void acquire() {
        var _ = revisions.acquire();
    }
}
