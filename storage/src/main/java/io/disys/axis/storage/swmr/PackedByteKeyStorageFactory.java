package io.disys.axis.storage.swmr;

public final class PackedByteKeyStorageFactory implements KeyStorageFactory<byte[]> {
    private final PackedByteComparator comparator;

    PackedByteKeyStorageFactory(PackedByteComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public KeyStorage<byte[]> single(byte[] key) {
        return PackedByteKeyStorage.of(key, comparator);
    }
}
