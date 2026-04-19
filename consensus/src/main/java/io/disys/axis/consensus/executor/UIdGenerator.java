package io.disys.axis.consensus.executor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates cluster-unique 64-bit IDs for commands, leases, and any other
 * entity requiring a node-scoped unique identifier.
 *
 * <h2>Layout</h2>
 * <pre>
 * | 16 bits nodeId | 40 bits timestamp ms | 8 bits counter |
 *       63..48             47..8                 7..0
 * </pre>
 *
 * <p>Only the lower 16 bits of the node's long ID are used — sufficient
 * for any realistic cluster size. The suffix (timestamp + counter) is
 * seeded once from the wall clock at construction and then incremented
 * atomically on every call — no further clock reads required.</p>
 *
 * <p>The counter allows 256 IDs per ms; overflow carries into the timestamp
 * field, borrowing from future milliseconds rather than wrapping. IDs
 * generated after a restart are numerically higher than pre-restart IDs
 * as long as at least 1 ms has elapsed between shutdown and startup.</p>
 */
public final class UIdGenerator {

    /**
     * Mask of 48 ones in the low bits — {@code (1L << 48) - 1}.
     * Applied to the incremented suffix before combining with the prefix
     * to prevent the counter from bleeding into bits 63..48 as it grows.
     */
    private static final long SUFFIX_MASK = (1L << 48) - 1;

    /** Node identity stamped into bits 63..48 of every generated ID. */
    private final long prefix;

    /** Monotonically increasing suffix; bits 47..8 hold timestamp ms, bits 7..0 hold the counter. */
    private final AtomicLong suffix;

    public UIdGenerator(long nodeId, long nowEpochMs) {
        // 0xFFFFL masks off all but the lower 16 bits of nodeId;
        // << 48 places those 16 bits at positions 63..48.
        this.prefix = (nodeId & 0xFFFFL) << 48;

        // (1L << 40) - 1: 40 ones — keeps only bits 39..0 of the epoch ms,
        // covering ~34 years before the timestamp portion wraps.
        // << 8 shifts the timestamp to bits 47..8, leaving bits 7..0 as
        // zero for the counter to increment from.
        long timestampMs = nowEpochMs & ((1L << 40) - 1);
        this.suffix = new AtomicLong(timestampMs << 8);
    }

    /**
     * Returns the next cluster-unique ID.
     *
     * <p>Atomically increments the suffix and combines it with the
     * fixed prefix. The suffix is masked to 48 bits before combining
     * to ensure it never overlaps with the node ID in bits 63..48.</p>
     *
     * @return a cluster-unique ID
     */
    public long next() {
        long s = suffix.incrementAndGet();
        return prefix | (s & SUFFIX_MASK);
    }
}
