package io.disys.axis.wal.api;

public record AsyncWalConfig(int maxPendingWrites, FlushStrategy flushStrategy) {
}
