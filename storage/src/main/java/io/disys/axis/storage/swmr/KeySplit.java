package io.disys.axis.storage.swmr;

public record KeySplit<K>(
        KeyStorage<K> left,
        KeyStorage<K> right,
        K promotedKey
) {
}