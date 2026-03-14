package consensus.cluster.membership;

/**
 * The type of change to apply to a single node's membership.
 *
 * @see MembershipChange
 * @see MembershipChanger
 */
public enum MembershipChangeType {

    /** Remove the node from the cluster entirely. */
    REMOVE,

    /** Add or promote the node as a voter. */
    ADD_VOTER,

    /** Add or demote the node as a learner (non-voting). */
    ADD_LEARNER,
}
