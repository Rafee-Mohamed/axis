package io.disys.axis.lease.model;

public sealed interface RenewResult {
    record LeaseNotFound(long id) implements RenewResult {}
    record LeaseRenewed(long id, long remainingTtl) implements RenewResult {}
    record LeaseRevokeInProgress(long id) implements RenewResult {}
    record LeaseCheckpoint(long id, long remainingTtl) implements RenewResult {}
}
