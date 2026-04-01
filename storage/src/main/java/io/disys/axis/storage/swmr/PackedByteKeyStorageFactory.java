package io.disys.axis.storage.swmr;

public final class PackedByteKeyStorageFactory implements KeyStorageFactory<byte[]> {
    @Override
    public KeyStorage<byte[]> single(byte[] key) {
        return new PackedByteKeyStorage(key, new int[]{0, key.length});
    }
}
