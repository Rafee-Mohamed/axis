package io.disys.axis.storage.swmr;

public record ChildrenSplit<K, V>(
        Children<K, V> left,
        Children<K, V> right
) {
}
