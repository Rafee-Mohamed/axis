package io.disys.axis.storage.swmr;

import java.util.Comparator;

public class ArrayKeyStorageFactory<K> implements KeyStorageFactory<K> {
    private final Comparator<K> comparator;

    public ArrayKeyStorageFactory(Comparator<K> comparator) {
        this.comparator = comparator;
    }

    @Override
    public KeyStorage<K> single(K key) {
        return ArrayKeyStorage.of(key, comparator);
    }
}
