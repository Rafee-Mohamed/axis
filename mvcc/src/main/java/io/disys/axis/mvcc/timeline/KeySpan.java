package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.*;

/**
 * One span of a key's existence within its timeline: a sequence of revisions
 * from creation through its last known mutation. A key may have multiple
 * spans if it has been deleted and recreated.
 */
public interface KeySpan {
    /** @return the commitSeq at which this span was created */
    long createdAt();

    /** @return the commitSeq of the last revision in this span */
    long modifiedAt();

    /** @return the last {@link Revision} in this span */
    Revision lastRevision();

    /** @return the first {@link Revision} in this span */
    Revision firstRevision();

    /** @return the version at the last revision in this span */
    int version();
}
