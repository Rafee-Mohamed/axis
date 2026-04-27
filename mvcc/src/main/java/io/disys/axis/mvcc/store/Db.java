package io.disys.axis.mvcc.store;

import io.disys.axis.backend.Database;

public record Db(Database revision, MetaDb meta) {
}
