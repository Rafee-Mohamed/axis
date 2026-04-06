package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;

import java.util.Arrays;

public record EncodedRevisionRecord(byte[] revision, byte[] record) implements Comparable<byte[]> {
    @Override
    public int compareTo(byte[] otherRevision) {
        return Arrays.compare(revision, otherRevision);
    }
}
