package io.disys.axis.wal;

public sealed interface FlushStrategy {
    record OnDemand() implements FlushStrategy {}
    record Periodic(long interval) implements FlushStrategy {}
}
