package io.disys.axis.mvcc.store;

import io.disys.axis.backend.Database;

/**
 * Pair of backend database handles used by the store.
 *
 * @param revision  stores encoded revision records, keyed by encoded {@link io.disys.axis.mvcc.model.Revision}
 * @param meta      stores store metadata: persisted commit seq, compaction boundary, and compaction state
 */
public record Db(Database revision, MetaDb meta) {
}
