package io.disys.axis.mvcc.model;

/** Iteration direction for ordered range scans and sorted queries. */
public enum SortDirection {

    /** Smallest to largest by the configured {@link SortTarget}. */
    ASCENDING,

    /** Largest to smallest by the configured {@link SortTarget}. */
    DESCENDING
}
