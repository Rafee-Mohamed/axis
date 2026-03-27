package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.List;

public class KeySpan {
    private final List<Revision> revisions;

    KeySpan(Revision createRevision) {
        revisions = new ArrayList<>();
        revisions.add(createRevision);
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

    Revision revision() {
        return revisions.getLast();
    }

    Revision firstRevision() {
        return revisions.getFirst();
    }

    int version() {
        return revisions.size() - 1;
    }

}
