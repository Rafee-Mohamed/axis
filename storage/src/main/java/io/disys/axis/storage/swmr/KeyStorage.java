package io.disys.axis.storage.swmr;

public interface KeyStorage<K> extends IndexedComparator<K> {

    K key(int idx);

    KeyStorage<K> insert(int idx, K key);

    KeySplit<K> insertAndSplit(int insertIdx, int splitIdx, K key);
}
