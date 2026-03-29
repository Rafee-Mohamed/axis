package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    Optional<DeadSpan> compact(long commitSeq) {
        var lb = lowerBound(commitSeq);

        if (lb <= 0) {
            return Optional.of(this);
        }

        if (lb >= revisions.size()) {
            return Optional.empty();
        }

        var compacted = new ArrayList<>(revisions.subList(lb, revisions.size()));

        return Optional.of(new DeadSpan(compacted));
    }

}
