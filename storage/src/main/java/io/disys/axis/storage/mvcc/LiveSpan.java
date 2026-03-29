package io.disys.axis.storage.mvcc;

import java.util.Optional;

public class LiveSpan implements KeySpan {
    private final VolatileList<Revision> revisions; 
    private final int position; 
    private long createdAt; 
    private int version;
    
    LiveSpan(VolatileList<Revision> revisions, int position, long createdAt, int version) {
        this.revisions = revisions;
        this.position = position;
        this.createdAt = createdAt;
        this.version = version;
    }

    static LiveSpan init(Revision revision) {
        return new LiveSpan(VolatileList.of(revision), 0, revision.commitSeq(), 0);
    }

    static LiveSpan empty(int position) {
        return new LiveSpan(VolatileList.allocate(10), position, -1, -1);
    }

    static LiveSpan create(VolatileList<Revision> revisions, int position, long createdAt, int version) {
        return new LiveSpan(revisions, position, createdAt, version);
    }
    
    // create and update revision
    void add(Revision revision) {
        if (isEmpty()) {
            createdAt = revision.commitSeq();
        }
        version += 1;
        revisions.add(revision);
    }

    @Override
    public long createdAtSeq() {
        return createdAt;
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
    public int version() {
        return version;
    }

    Revision get(int idx) {
        return revisions.get(idx);
    }

    int size() { return revisions.size(); }

    DeadSpan complete(Revision deleteRevision) {
        var nextDeadSpan = revisions.toList();
        nextDeadSpan.add(deleteRevision);
        return DeadSpan.create(nextDeadSpan, createdAt, version);
    }

    void release() {
        revisions.release();
    }

    void acquire() {
        var _ = revisions.acquire();
    }

    int lowerBound(long commitSeq) {
        var left = 0;
        var right = revisions.size() - 1;

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var rev = revisions.get(mid);

            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    Optional<LiveSpan> compact(long commitSeq) {
        var lb = lowerBound(commitSeq);

        if (lb <= 0) {
            return Optional.of(this);
        }

        if (lb >= revisions.size()) {
            return Optional.empty();
        }

        var compacted = revisions.copy(lb);;

        return Optional.of(new LiveSpan(compacted, position, createdAt, version));
    }
}
