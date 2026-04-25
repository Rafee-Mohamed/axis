package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public record DeadSpan(List<Revision> revisions, long createdAt, int startVersion) implements KeySpan {

    // createdAt - at which commitSeq this span is created at
    // at which version is this span is in currently - the last revision's version
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

    public int versionAt(int idx) { return startVersion + idx; }

    public Revision get(int idx) {
        return revisions.get(idx);
    }

    public int size() {
        return revisions.size();
    }


    Optional<DeadSpan> compact(long commitSeq) {
        var floor = Query.floorRevision(revisions, commitSeq);

        if (floor < 0) {
            return Optional.of(this);
        }

        // if the floor is last of revisions and the commitSeq is behind
        // the compact commitSeq then that revision does not exist at
        // compact commitSeq
        if (floor == revisions.size() - 1 && revisions.getLast().compareTo(commitSeq) < 0) {
            return Optional.empty();
        }

        var compacted = new ArrayList<>(revisions.subList(floor, revisions.size()));

        return Optional.of(new DeadSpan(compacted, createdAt, startVersion + floor));
    }

}
