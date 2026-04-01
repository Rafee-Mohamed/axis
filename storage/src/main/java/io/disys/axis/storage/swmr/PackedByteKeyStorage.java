package io.disys.axis.storage.swmr;

import java.util.Arrays;

public class PackedByteKeyStorage implements KeyStorage<byte[]> {

    // key at ith position -> keys[offsets[i] ... offsets[i+1])
    private final byte[] keys; // size k
    private final int[] offsets; // size k + 1

    public PackedByteKeyStorage(byte[] keys, int[] offsets) {
        this.keys = keys;
        this.offsets = offsets;
    }


    @Override
    public int size() {
        return offsets.length - 1;
    }

    @Override
    public byte[] key(int idx) {
        return Arrays.copyOfRange(keys, offsets[idx], offsets[idx + 1]);
    }

    @Override
    public int compare(int idx, byte[] key) {
        return 0;
    }

    @Override
    public KeyStorage<byte[]> insert(int idx, byte[] key) {
        return null;
    }

    @Override
    public Split<byte[]> insertAndSplit(int insertIdx, int splitIdx, byte[] key) {
        return null;
    }
}
