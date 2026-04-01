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
        return null;
    }

    @Override
    public Split<byte[]> insertAndSplit(int insertIdx, int splitIdx, byte[] key) {
        return null;
    }


    private void checkBounds(int idx) {
        if (idx < 0 || idx >= size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + size() + ")");
        }
    }
}
