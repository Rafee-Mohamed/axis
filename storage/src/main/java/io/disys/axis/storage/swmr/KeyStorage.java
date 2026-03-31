package io.disys.axis.storage.swmr;

public interface KeyStorage<K> {
    int size();

    K keyAt(int index);

    int compareAt(int index, K key);

    KeyStorage<K> insertAt(int index, K key);

    Split<K> split(int mid);
}
