package io.disys.axis.wal.api;

public sealed interface FlushStrategy {
    record OnDemand() implements FlushStrategy {}
    record Periodic(long interval) implements FlushStrategy {}
}
