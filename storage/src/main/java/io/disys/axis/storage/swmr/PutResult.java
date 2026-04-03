package io.disys.axis.storage.swmr;

public sealed interface PutResult<K, V> {

    V replaced();

    record NoSplit<K, V>(
            Node<K, V> node,
            V replaced
    ) implements PutResult<K, V> {}

    record Split<K, V>(
            Node<K, V> left,
            Node<K, V> right,
            K promotedKey,
            V replaced
    ) implements PutResult<K, V> {}
}