package io.disys.axis.wal;

public record AsyncWalConfig(int maxPendingWrites, FlushStrategy flushStrategy) {
}
