package io.disys.axis.lease.model;

public sealed interface GrantResult {
    record LeaseGranted(long id) implements GrantResult {}
    record LeaseAlreadyExists(long id) implements GrantResult {}
}
