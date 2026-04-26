package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A completed {@link KeySpan}: the key was created and subsequently deleted.
 * Contains at least two revisions - the creation and the deletion.
 *
 * @param revisions     the ordered list of revisions in this span
 * @param createdAt     the commitSeq at which this span was created
 * @param startVersion  the version of the first revision currently in this span
 */
public record DeadSpan(List<Revision> revisions, long createdAt, int startVersion) implements KeySpan {

    /**
     * @throws IllegalStateException if {@code revisions} contains fewer than two entries
     */
    static DeadSpan create(List<Revision> revisions, long createdAt, int startVersion) {
        if (revisions.size() < 2) {
            throw new IllegalStateException("Dead span should have at lease 2 revisions - create and delete");
        }
        return new DeadSpan(revisions, createdAt, startVersion);
    }

    @Override
    public long modifiedAt() {
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
    public int version() {
        return startVersion + revisions.size() - 1;
    }

    /** @return the version of the revision at index {@code idx} */
    public int versionAt(int idx) { return startVersion + idx; }

    /** @return the {@link Revision} at index {@code idx} */
    public Revision get(int idx) {
        return revisions.get(idx);
    }

    /** @return the number of revisions currently in this span */
    public int size() {
        return revisions.size();
    }

    /**
     * Retains the revision visible at {@code commitSeq} and everything after it,
     * dropping earlier history. A dead span is fully discarded when the key was
     * already deleted before {@code commitSeq} with no revisions after — it has
     * no presence at or beyond the compaction point.
     *
     * @param commitSeq  the compaction boundary
     * @return the compacted span, or {@link Optional#empty()} if the key has no
     *         presence at or after {@code commitSeq}
     */
    Optional<DeadSpan> compact(long commitSeq) {
        var floor = Query.floorRevision(revisions, commitSeq);

        if (floor < 0) {
            return Optional.of(this);
        }

        // floor lands on the deletion (last revision) and it is strictly before
        // commitSeq — the key was already gone at the compaction point with nothing after
        //
        // span [r3, r6, r9], r9 is the deletion:
        //   compact(2)  -> floor < 0, all revisions after compaction point -> unchanged
        //   compact(7)  -> floor = r6, drop r3, keep [r6, r9]
        //   compact(9)  -> floor = r9, r9 == commitSeq, keep [r9]
        //   compact(10) -> floor = r9, r9 < commitSeq                      -> discard
        if (floor == revisions.size() - 1 && revisions.getLast().compareTo(commitSeq) < 0) {
            return Optional.empty();
        }

        var compacted = new ArrayList<>(revisions.subList(floor, revisions.size()));

        return Optional.of(new DeadSpan(compacted, createdAt, startVersion + floor));
    }

}
