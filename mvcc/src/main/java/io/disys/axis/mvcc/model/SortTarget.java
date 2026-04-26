package io.disys.axis.mvcc.model;

/** The record field by which range query results are sorted. */
public enum SortTarget {

    /** Lexicographic order on raw key bytes. Default sort target. */
    KEY,

    /** Modification count within the key's current lifetime. */
    VERSION,

    /** {@code commitSeq} at which this key was first created in its current lifetime. */
    CREATED_REVISION,

    /** {@code commitSeq} of the most recent write to this key, including deletion. */
    MODIFIED_REVISION,

    /** Lexicographic order on raw value bytes. */
    VAL
}
