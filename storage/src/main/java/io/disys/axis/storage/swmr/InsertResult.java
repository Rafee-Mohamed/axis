package io.disys.axis.storage.swmr;

public sealed interface InsertResult<K, V> {
    record NoSplit<K, V>(
            Node<K, V> left,
            Node<K, V> right,
            K promotedKey
    ) implements InsertResult<K, V> {}

    record Split<K, V>(
            Node<K, V> left,
            Node<K, V> right,
            K promotedKey
    ) implements InsertResult<K, V> {}
}