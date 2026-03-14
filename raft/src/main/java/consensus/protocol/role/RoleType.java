package consensus.protocol.role;

/**
 * Identifies the current protocol role of a Raft node.
 *
 * <p>Distinct from {@link consensus.cluster.membership.MemberType},
 * which classifies a node's static membership (voter vs learner).
 * A voter can be in any of the four voter roles ({@code FOLLOWER},
 * {@code PRE_CANDIDATE}, {@code CANDIDATE}, {@code LEADER}); a
 * learner is always {@code LEARNER}.</p>
 */
public enum RoleType {
    /** Non-voting member that only replicates entries. */
    LEARNER,
    /** Elected leader responsible for log replication and coordination. */
    LEADER,
    /** Voting member passively replicating from the leader. */
    FOLLOWER,
    /** Voting member running a pre-election round before a real election. */
    PRE_CANDIDATE,
    /** Voting member running a real election to become leader. */
    CANDIDATE
}
