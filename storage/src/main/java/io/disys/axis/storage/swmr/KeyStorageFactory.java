package io.disys.axis.storage.swmr;

public interface KeyStorageFactory<K> {
    KeyStorage<K> single(K key);
}