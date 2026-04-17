package io.disys.axis.lease.model;

public sealed interface KeyAttachResult {
    record LeaseNotFound(long id) implements KeyAttachResult {}
    record Attached(long id) implements KeyAttachResult {}

}