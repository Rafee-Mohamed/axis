package io.disys.axis.lease.model;

public sealed interface KeyDetachResult {
    record LeaseNotFound(long id) implements KeyDetachResult {}
    record Detached(long id) implements KeyDetachResult {}

}
