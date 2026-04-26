package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Revision;

/**
 * Revision metadata for a single mutation.
 *
 * @param revision   the revision of this mutation
 * @param createdAt  the commitSeq at which this key span was created
 * @param version    the mutation count at this revision
 */
public record RevisionData(
        Revision revision,
        long createdAt,
        int version
) {
    /** @return {@code revision.commitSeq()} */
    public long modifiedAt() {
        return revision.commitSeq();
    }
}
