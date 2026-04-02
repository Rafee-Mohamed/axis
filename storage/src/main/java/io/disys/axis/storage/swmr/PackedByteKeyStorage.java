package io.disys.axis.storage.swmr;

import java.util.Arrays;

public class PackedByteKeyStorage implements KeyStorage<byte[]> {

    // key at ith position -> keys[offsets[i] ... offsets[i+1])
    private final byte[] keys; // size k
    private final int[] offsets; // size k + 1
    private final PackedByteComparator comparator;

    public PackedByteKeyStorage(byte[] keys, int[] offsets, PackedByteComparator comparator) {
        this.keys = keys;
        this.offsets = offsets;
        this.comparator = comparator;
    }

    static PackedByteKeyStorage of(byte[] key, PackedByteComparator comparator) {
        var offsets = new int[]{0, key.length};
        return new PackedByteKeyStorage(Arrays.copyOf(key, key.length), offsets, comparator);
    }

    @Override
    public int size() {
        return offsets.length - 1;
    }

    @Override
    public byte[] key(int idx) {
        checkBounds(idx);
        return Arrays.copyOfRange(keys, offsets[idx], offsets[idx + 1]);
    }

    @Override
    public int compare(int idx, byte[] key) {
        checkBounds(idx);
        return comparator.compare(keys, offsets[idx], offsets[idx + 1], key);
    }

    @Override
    public KeyStorage<byte[]> insert(int idx, byte[] key) {
        checkInsertBounds(idx);

        var newKeysSize = keys.length + key.length;
        var newKeys = new byte[newKeysSize];
        var insertPos = offsets[idx];

        System.arraycopy(keys, 0, newKeys, 0, insertPos);
        System.arraycopy(key, 0, newKeys, insertPos, key.length);
        System.arraycopy(keys, insertPos, newKeys, insertPos + key.length, keys.length - insertPos);

        var newOffsets = new int[offsets.length + 1];

        System.arraycopy(offsets, 0, newOffsets, 0, idx + 1);
        for (var i = idx; i < offsets.length; i++) {
            newOffsets[i + 1] = offsets[i] + key.length;
        }

        return new PackedByteKeyStorage(newKeys, newOffsets, comparator);
    }

    @Override
    public KeySplit<byte[]> insertAndSplit(int insertIdx, int splitIdx, byte[] key) {
        checkInsertBounds(insertIdx);
        checkSplitBounds(splitIdx);

        // ['a', 'b', 'c', 'e']
        // [0,  1,  2,  3,  4]
        // key = 'd'
        // insertIdx = 3
        // splitIdx = 2

        if (insertIdx >= splitIdx) {
            var leftKeys = new byte[offsets[splitIdx]];
            System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);

            var leftOffsets = new int[splitIdx + 1];
            System.arraycopy(offsets, 0, leftOffsets, 0, leftOffsets.length);

            var leftKeyStorage = new PackedByteKeyStorage(leftKeys, leftOffsets, comparator);

            var rightKeys = new byte[keys.length - offsets[splitIdx] + key.length];

            // prefix, newKey, suffix
            // offsets -> [splitIdx, insertIdx) [insertIdx, insertIdx + key.length) [insertIdx + key.length, keys.length + key.length)

            var prefixStart = offsets[splitIdx];
            var newKeyStart = offsets[insertIdx];
            var prefixLen = newKeyStart - prefixStart;
            var suffixStart = prefixLen + key.length;
            var suffixLen = keys.length - newKeyStart;

            System.arraycopy(keys, prefixStart, rightKeys, 0, prefixLen);
            System.arraycopy(key, 0, rightKeys, prefixLen, key.length);
            System.arraycopy(keys, newKeyStart, rightKeys,  suffixStart, suffixLen);

            var rightOffsets = new int[offsets.length - splitIdx + 1];

            // prefix offset
            for (var i = splitIdx; i <= insertIdx; i++) {
                rightOffsets[i - splitIdx] = offsets[i] - prefixStart;
            }

            // insert offset
            rightOffsets[insertIdx - splitIdx + 1] = rightOffsets[insertIdx - splitIdx] + key.length;

            // suffix offset
            for (var i = insertIdx + 1; i < offsets.length; i++) {
                rightOffsets[i - splitIdx + 1] = offsets[i] - prefixStart + key.length;
            }

            var rightKeyStorage = new PackedByteKeyStorage(rightKeys, rightOffsets, comparator);

            return new KeySplit<>(leftKeyStorage, rightKeyStorage, rightKeyStorage.key(0));
        }

        var leftKeys = new byte[offsets[splitIdx] + key.length];

        var prefixStart = 0;
        var newKeyStart = offsets[insertIdx];
        var prefixLen = newKeyStart;
        var suffixStart = prefixLen + key.length;
        var suffixLen = keys.length - newKeyStart;

        System.arraycopy(keys, prefixStart, leftKeys, 0, prefixLen);
        System.arraycopy(key, 0, leftKeys, prefixLen, key.length);
        System.arraycopy(keys, newKeyStart, leftKeys,  suffixStart, suffixLen);

        var leftOffsets = new int[splitIdx + 1];

        // prefix offset
        System.arraycopy(offsets, 0, leftOffsets, 0, insertIdx + 1);

        // insert offset
        leftOffsets[insertIdx + 1] = leftOffsets[insertIdx] + key.length;

        // suffix offset
        for (var i = insertIdx + 1; i < offsets.length; i++) {
            leftOffsets[i + 1] = offsets[i] + key.length;
        }

        var leftKeyStorage = new PackedByteKeyStorage(leftKeys, leftOffsets, comparator);

        var splitIdxAfterInsertion = splitIdx - 1;
        var rightKeys = new byte[keys.length - offsets[splitIdxAfterInsertion]];
        System.arraycopy(keys, offsets[splitIdxAfterInsertion], rightKeys, 0, rightKeys.length);

        var rightOffsets = new int[offsets.length - splitIdxAfterInsertion];
        for (var i = 0; i < rightOffsets.length; i++) {
            rightOffsets[i] = offsets[i + splitIdxAfterInsertion] - offsets[splitIdxAfterInsertion];
        }

        var rightKeyStorage = new PackedByteKeyStorage(rightKeys, rightOffsets, comparator);

        // splitIdx is checked to be within bounds - 0 < splitIdx <= size
        // therefore, at splitIdx a key will be present which is the first key
        // of rightKeyStorage, so key(0) won't fail.
        return new KeySplit<>(leftKeyStorage, rightKeyStorage, rightKeyStorage.key(0));
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

    private void checkBounds(int idx) {
        if (idx < 0 || idx >= size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + size() + ")");
        }
    }
}
