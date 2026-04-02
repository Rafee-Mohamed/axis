package io.disys.axis.storage.swmr;

public interface IndexedComparator<K> {
    int size();
    int compare(int idx, K key);
}
