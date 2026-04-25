package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Revision;

public record RevisionData(
        Revision revision,
        long createdAt,
        int version
) {
    public long modifiedAt() {
        return revision.commitSeq();
    }
}
