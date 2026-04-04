package io.disys.axis.storage.swmr;

import java.util.Arrays;
import java.util.Comparator;

public class ArrayKeyStorage<K> implements KeyStorage<K> {
    private final K[] keys;
    private final Comparator<K> comparator;


    public ArrayKeyStorage(K[] keys, Comparator<K> comparator) {
        this.keys = keys;
        this.comparator = comparator;
    }
    
    static <K> ArrayKeyStorage<K> of(K key, Comparator<K> comparator) {
        return new ArrayKeyStorage<>((K[]) new Object[]{key}, comparator);
    }

    @Override
    public int size() {
        return keys.length;
    }

    @Override
    public int compare(int idx, K key) {
        checkIndexBounds(idx);
        return comparator.compare(keys[idx], key);
    }

    @Override
    public K key(int idx) {
        checkIndexBounds(idx);
        return keys[idx];
    }

    @Override
    public KeyStorage<K> insert(int idx, K key) {
        checkInsertBounds(idx);
        var newKeys = (K[]) new Object[keys.length + 1];

        System.arraycopy(keys, 0, newKeys, 0, idx);
        newKeys[idx] = key;
        System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);

        return new ArrayKeyStorage<K>(newKeys, comparator);
    }

    @Override
    public KeyStorage<K> remove(int idx) {
        checkIndexBounds(idx);
        var newKeys = (K[]) new Object[keys.length - 1];

        System.arraycopy(keys, 0, newKeys, 0, idx);
        System.arraycopy(keys, idx + 1, newKeys, idx, keys.length - idx - 1);

        return new ArrayKeyStorage<>(newKeys, comparator);
    }

    @Override
    public KeyStorage<K> replace(int idx, K key) {
        checkIndexBounds(idx);

        var newKeys = Arrays.copyOf(keys, keys.length);
        newKeys[idx] = key;

        return new ArrayKeyStorage<>(newKeys, comparator);
    }

    @Override
    public KeySplit<K> split(int idx) {
        checkSplitBounds(idx);
        var leftKeys = (K[]) new Object[idx];
        System.arraycopy(keys, 0, leftKeys, 0, idx);

        var rightKeys = (K[]) new Object[keys.length - idx];
        System.arraycopy(keys, idx, rightKeys, 0, rightKeys.length);
        
        var leftKeyStorage = new ArrayKeyStorage<>(leftKeys, comparator);
        var rightKeyStorage = new ArrayKeyStorage<>(rightKeys, comparator);
        return new KeySplit<>(leftKeyStorage, rightKeyStorage, rightKeyStorage.key(0));
    }

    @Override
    public KeyStorage<K> merge(KeyStorage<K> other) {
        if (!(other instanceof ArrayKeyStorage<K> otherStorage)) {
            throw new IllegalArgumentException("Incompatible KeyStorage to merge");
        }
        var newKeys = (K[]) new Object[keys.length + otherStorage.size()];
        System.arraycopy(keys, 0, newKeys, 0, keys.length);
        System.arraycopy(otherStorage.keys, 0, newKeys, keys.length, otherStorage.size());

        return new ArrayKeyStorage<>(newKeys, comparator);
    }

    @Override
    public KeySplit<K> insertAndSplit(int insertIdx, int splitIdx, K key) {
        checkInsertBounds(insertIdx);
        checkSplitBounds(splitIdx);

        if (insertIdx >= splitIdx) {
            var leftKeys = (K[]) new Object[splitIdx];
            System.arraycopy(keys, 0, leftKeys, 0, splitIdx);

            var rightKeys = (K[]) new Object[keys.length - splitIdx + 1];
            var prefixLen = insertIdx - splitIdx;
            var suffixLen = keys.length - insertIdx;

            System.arraycopy(keys, splitIdx, rightKeys, 0, prefixLen);
            rightKeys[prefixLen] = key;
            System.arraycopy(keys, insertIdx, rightKeys, prefixLen + 1, suffixLen);

            var leftKeyStorage = new  ArrayKeyStorage<>(leftKeys, comparator);
            var rightKeyStorage = new  ArrayKeyStorage<>(rightKeys, comparator);
            return new KeySplit<>(leftKeyStorage, rightKeyStorage, rightKeyStorage.key(0));
        }

        var leftKeys = (K[]) new Object[splitIdx + 1];
        System.arraycopy(keys, 0, leftKeys, 0, insertIdx);
        leftKeys[insertIdx] = key;
        System.arraycopy(keys, insertIdx, leftKeys, insertIdx + 1, splitIdx - insertIdx);

        var splitIdxAfterInsertion = splitIdx - 1;
        var rightKeys = (K[]) new Object[keys.length - splitIdxAfterInsertion];
        System.arraycopy(keys, splitIdxAfterInsertion, rightKeys, 0, rightKeys.length);

        var leftKeyStorage = new  ArrayKeyStorage<>(leftKeys, comparator);
        var rightKeyStorage = new  ArrayKeyStorage<>(rightKeys, comparator);
        return new KeySplit<>(leftKeyStorage, rightKeyStorage, rightKeyStorage.key(0));
    }

    @Override
    public KeyStorage<K> removeAndInsert(int removeIdx, int insertIdx, K key) {
        checkIndexBounds(removeIdx);
        checkIndexBounds(insertIdx);

        var newKeys = (K[]) new Object[keys.length];

        // prefix is unchanged before insertIdx or removeIdx
        var unchangedPrefixEnd = Math.min(removeIdx, insertIdx);
        System.arraycopy(keys, 0, newKeys, 0, unchangedPrefixEnd);

        if (insertIdx > removeIdx) {
            System.arraycopy(keys, removeIdx + 1, newKeys, removeIdx, insertIdx - removeIdx - 1);
            newKeys[insertIdx] = key;
            System.arraycopy(keys, insertIdx, newKeys, insertIdx + 1, keys.length - insertIdx);
        } else { // insert then remove, insertIdx == prefixEnd
            newKeys[insertIdx] = key;
            System.arraycopy(keys, insertIdx, newKeys, insertIdx + 1, removeIdx - insertIdx);
            System.arraycopy(keys, removeIdx + 1, newKeys, removeIdx, keys.length - removeIdx - 1);
        }

        return new  ArrayKeyStorage<>(newKeys, comparator);
    }

    @Override
    public KeyStorage<K> insertAndMerge(int insertIdx, K key, KeyStorage<K> other) {
        checkInsertBounds(insertIdx);

        if (!(other instanceof ArrayKeyStorage<K> otherStorage)) {
            throw new IllegalArgumentException("Incompatible KeyStorage to merge");
        }

        var otherKeys = otherStorage.keys;
        var newKeys = (K[]) new Object[keys.length + otherKeys.length + 1];

        System.arraycopy(keys, 0, newKeys, 0, insertIdx);
        newKeys[insertIdx] = key;
        System.arraycopy(keys, insertIdx, newKeys, insertIdx + 1, keys.length - insertIdx);

        System.arraycopy(otherKeys, 0, newKeys, keys.length + 1, otherKeys.length);

        return new  ArrayKeyStorage<>(newKeys, comparator);
    }

    private void checkSplitBounds(int idx) {
        if (idx <= 0 || idx > size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for split: " + "(" + 0 + " " + size() + ")");
        }
    }

    private void checkInsertBounds(int idx) {
        if (idx < 0 || idx > size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for insert: " + "[" + 0 + " " + size() + ")");
        }
    }

    private void checkIndexBounds(int idx) {
        if (idx < 0 || idx >= size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + size() + ")");
        }
    }
    
}
