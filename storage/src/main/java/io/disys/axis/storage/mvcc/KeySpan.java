package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class KeySpan {
    private final List<Revision> revisions;

    private KeySpan(List<Revision> revisions) {
        this.revisions = revisions;
    }

    static KeySpan init(Revision createRevision) {
        var revisions = new ArrayList<Revision>();
        revisions.add(createRevision);
        return new KeySpan(revisions);
    }

    static KeySpan dead(List<Revision> revisions) {
        if (revisions.size() < 2) {
            throw new IllegalStateException("Dead span should have at lease 2 revisions - create and delete");
        }
        return new KeySpan(revisions);
    }

    void add(Revision updateRevision) {
        revisions.add(updateRevision);
    }

    long createdAtSeq() {
        return revisions.getFirst().commitSeq();
    }

    long modifiedAtSeq() {
        return revisions.getLast().commitSeq();
    }

    Revision lastRevision() {
        return revisions.getLast();
    }

    Revision firstRevision() {
        return revisions.getFirst();
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

    Optional<KeySpan> compact(long commitSeq) {
        var lb = lowerBound(commitSeq);

        if (lb <= 0) {
            return Optional.of(this);
        }

        if (lb >= revisions.size()) {
            return Optional.empty();
        }

        var compacted = new ArrayList<>(revisions.subList(lb, revisions.size()));

        return Optional.of(new KeySpan(compacted));
    }

    int version() {
        return revisions.size() - 1;
    }

}
