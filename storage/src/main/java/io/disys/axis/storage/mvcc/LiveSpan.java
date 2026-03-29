package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.Optional;

public record LiveSpan(int position, VolatileList<Revision> revisions) implements KeySpan {

    static LiveSpan init(Revision revision) {
        return new LiveSpan(0, VolatileList.of(revision));
    }
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

        var compacted = revisions().copy(lb);;

        return Optional.of(new LiveSpan(position, compacted));
    }
}
