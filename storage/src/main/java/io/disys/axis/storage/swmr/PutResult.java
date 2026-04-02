package io.disys.axis.storage.swmr;

public sealed interface PutResult<K, V> {
    record NoSplit<K, V>(
            Node<K, V> node
    ) implements PutResult<K, V> {}

    record Split<K, V>(
            Node<K, V> left,
            Node<K, V> right,
            K promotedKey
    ) implements PutResult<K, V> {}
}