package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.model.*;

public interface VersionedStore {
    // Multi thread access
    // multiple readers allowed, can called by multiple threads to get readers
    Reader reader();

    // Only single thread access for writer/compact/sync

    // Behaviour of concurrent threads accessing these are undefined
    // compact removes all revisions existed before given commitSeq
    // that are not present as the point in time view of commitSeq
    Writer writer();

    CompactResult compact(long commitSeq);

    void sync();
}
