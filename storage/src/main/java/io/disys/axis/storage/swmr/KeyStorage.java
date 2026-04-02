package io.disys.axis.storage.swmr;

public interface KeyStorage<K> {
    int size();

    K key(int idx);

    int compare(int idx, K key);

    KeyStorage<K> insert(int idx, K key);

    KeySplit<K> insertAndSplit(int insertIdx, int splitIdx, K key);
}
