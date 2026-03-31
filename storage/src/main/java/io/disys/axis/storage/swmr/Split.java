package io.disys.axis.storage.swmr;

public record Split<K>(
        KeyStorage<K> left,
        KeyStorage<K> right,
        K promotedKey
) {
}