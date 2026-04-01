package io.disys.axis.storage.swmr;

@FunctionalInterface
public interface PackedByteComparator {
    int compare(byte[] bytes, int start, int end, byte[] key);
}
