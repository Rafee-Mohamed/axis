package io.disys.axis.storage.swmr;

public record ValueSplit<V>(
        ValueStorage<V> left,
        ValueStorage<V> right
) {
}
