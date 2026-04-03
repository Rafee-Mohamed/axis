package io.disys.axis.storage.swmr;

public interface KeyStorage<K> extends IndexedComparator<K> {

    K key(int idx);

    KeyStorage<K> insert(int idx, K key);

    KeyStorage<K> remove(int idx);

    KeyStorage<K> replace(int idx, K key);

    KeySplit<K> split(int idx);

    KeyStorage<K> merge(KeyStorage<K> other);

    default KeySplit<K> insertAndSplit(int insertIdx, int splitIdx, K key) {
        return insert(insertIdx, key).split(splitIdx);
    }

    default KeyStorage<K> removeAndInsert(int removeIdx, int insertIdx, K key) {
        return remove(removeIdx).insert(insertIdx, key);
    }

    default KeyStorage<K> insertAndMerge(int insertIdx, K key, KeyStorage<K> other) {
        return insert(insertIdx, key).merge(other);
    }
}
