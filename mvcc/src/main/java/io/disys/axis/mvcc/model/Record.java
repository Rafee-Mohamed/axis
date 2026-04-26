package io.disys.axis.mvcc.model;

import io.disys.axis.mvcc.timeline.KeySpan;

/**
 * The value payload stored for a key at a specific revision.
 *
 * <p>A record is either a live record or a tombstone. A tombstone marks a
 * deletion: its {@code val} is empty and {@code tombstone} is {@code true}.
 * Tombstones are stored so that the deletion event and the key's final version
 * metadata are observable at the revision the key was deleted.</p>
 *
 * @param key            the key this record belongs to
 * @param val            the stored value; {@code byte[0]} for tombstones
 * @param tombstone      {@code true} if this record marks a deletion
 * @param version        monotonically increasing count of modifications to this key
 *                       within its current lifetime; starts at {@code 1} on creation,
 *                       increments on each write including deletion, and resets to
 *                       {@code 1} if the key is recreated after deletion
 * @param createdAtSeq   the {@code commitSeq} at which this key was first created
 *                       in its current lifetime
 * @param modifiedAtSeq  the {@code commitSeq} of the most recent write, including
 *                       deletion
 */
public record Record(
        byte[] key,
        byte[] val,
        boolean tombstone,
        int version,
        long createdAtSeq,
        long modifiedAtSeq
) {
    /**
     * Creates a live record, deriving {@code version} and timestamps from {@code span}.
     *
     * @param key   the key
     * @param val   the value
     * @param span  the key's current {@link KeySpan} at write time
     */
    public Record(byte[] key, byte[] val, KeySpan span) {
        this(key, val, false, span.version(), span.createdAt(), span.modifiedAt());
    }

    /**
     * Creates a tombstone, deriving {@code version} and timestamps from {@code span}
     * at the time of deletion.
     *
     * @param key   the key being deleted
     * @param span  the key's {@link KeySpan} at deletion time
     */
    public Record(byte[] key, KeySpan span) {
        this(key, new byte[0], true, span.version(), span.createdAt(), span.modifiedAt());
    }
}
