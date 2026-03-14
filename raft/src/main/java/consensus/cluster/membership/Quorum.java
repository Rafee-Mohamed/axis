package consensus.cluster.membership;

/**
 * Outcome of a quorum check (vote or commit).
 *
 * <p>
 * Ordered from worst to best so that {@link Comparable} semantics
 * align with the quorum calculation: the minimum of two results is
 * the more conservative outcome, which is what joint consensus needs.
 * </p>
 *
 * @see MajorityConfig#quorumResult
 * @see JointConfig#quorumResult
 */
public enum Quorum {

    /** A majority has voted against - quorum cannot be reached. */
    NOT_REACHED,

    /** Not enough votes yet to decide either way. */
    PENDING,

    /** A majority has agreed - quorum is reached. */
    REACHED
}
