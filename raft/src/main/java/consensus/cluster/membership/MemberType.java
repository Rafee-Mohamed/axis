package consensus.cluster.membership;

/**
 * A node's static membership status in the cluster.
 *
 * <p>Distinct from the protocol role (Leader, Follower, Candidate, etc.)
 * which is dynamic. A node's membership type determines whether it
 * participates in elections and commit quorums.</p>
 *
 * @see MembershipConfig#memberType(consensus.core.NodeId)
 */
public enum MemberType {

    /** Full cluster member - participates in elections and commit quorums. */
    VOTER,

    /** Non-voting member - receives log replication but does not vote. */
    LEARNER
}
