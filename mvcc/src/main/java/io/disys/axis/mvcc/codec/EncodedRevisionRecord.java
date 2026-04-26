package io.disys.axis.mvcc.codec;

import io.disys.axis.mvcc.model.*;

import java.util.Arrays;

/**
 * The binary encoding of a {@link RevisionRecord}: an encoded revision key
 * paired with its encoded record value.
 *
 * <p>Implements {@link Comparable}{@code <byte[]>} via lexicographic comparison
 * of the revision bytes. Because {@code commitSeq} and {@code ordinal} are encoded
 * as big-endian integers, this preserves the natural total order of
 * {@link Revision}.</p>
 */
public record EncodedRevisionRecord(byte[] revision, byte[] record) implements Comparable<byte[]> {

    /**
     * Compares the encoded revision bytes of this record against {@code otherRevision}
     * lexicographically.
     *
     * @param otherRevision  the encoded revision bytes to compare against
     * @return per {@link Comparable} contract
     */
    @Override
    public int compareTo(byte[] otherRevision) {
        return Arrays.compare(revision, otherRevision);
    }
}
