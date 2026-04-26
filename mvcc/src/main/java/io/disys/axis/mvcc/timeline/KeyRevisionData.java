package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Revision;

/**
 * A {@link RevisionData} paired with its key.
 *
 * @param key   the key
 * @param data  the revision metadata
 */
public record KeyRevisionData(
        byte[] key,
       RevisionData data
) {
    public Revision revision() {
        return data.revision();
    }

    public long modifiedAt() {
        return data.modifiedAt();
    }

    public long createdAt() {
        return data.createdAt();
    }

    public int version() {
        return data.version();
    }
}
