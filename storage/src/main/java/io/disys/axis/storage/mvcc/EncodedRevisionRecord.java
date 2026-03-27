package io.disys.axis.storage.mvcc;

import java.util.Arrays;

public record EncodedRevisionRecord(byte[] revision, byte[] record) implements Comparable<byte[]> {
    @Override
    public int compareTo(byte[] otherRevision) {
        return Arrays.compare(revision, otherRevision);
    }
}
