package io.disys.axis.lease.model;

public sealed interface RevokeResult {
    record LeaseRevoked(long id) implements RevokeResult {}
    record LeaseNotFound(long id) implements RevokeResult {}
    record LeaseRevokeInProgress(long id) implements RevokeResult {}
}
