package io.disys.axis.storage.swmr;

public sealed interface Node<K, V> {
    KeyStorage<K> keys();
    record Internal<K, V>(
            KeyStorage<K> keys,
            Node<K, V>[] children
    ) implements Node<K, V> {
    }

    record Leaf<K, V>(
            KeyStorage<K> keys,
            ValueStorage<V> values
    ) implements Node<K, V> {
    }
}
