package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

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
        return new LiveSpan(VolatileList.allocate(10), position, -1, 0);
    }

    static LiveSpan create(VolatileList<Revision> revisions, int position, long createdAt, int version) {
        return new LiveSpan(revisions, position, createdAt, version);
    }

    int position() { return position; }

    VolatileList<Revision> revisions() { return revisions; }

    VolatileList<Revision>.PinnedView pin() {
        return revisions.pin();
    }
    
    // create and update revision
    void add(Revision revision) {
        if (isEmpty()) {
            createdAt = revision.commitSeq();
        }
        version++;
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
        version++;
        var nextDeadSpan = revisions.toList();
        nextDeadSpan.add(deleteRevision);
        return DeadSpan.create(nextDeadSpan, createdAt, version);
    }


    Optional<LiveSpan> compact(long commitSeq) {
        var lb = Query.lowerBoundRevision(revisions, commitSeq);

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
