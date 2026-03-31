package io.disys.axis.storage.swmr;

public sealed interface Node<K, V> {
    record Internal<K, V>(
            KeyStorage<K> keys,
            Node<K, V>[] children
    ) implements Node<K, V> {
    }

    record Leaf<K, V>(
            KeyStorage<K> keys,
            Object[] values
    ) implements Node<K, V> {
    }
}
