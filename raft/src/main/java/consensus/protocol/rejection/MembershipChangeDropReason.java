package consensus.protocol.rejection;

/**
 * Why a membership change proposal was rejected by the protocol.
 */
public enum MembershipChangeDropReason {

    /** No leader is known - cannot forward or accept the change. */
    NO_LEADER,

    /** A leadership transfer is in progress - the leader is not accepting changes. */
    LEADER_TRANSFER_IN_PROGRESS,

    /** A previous membership change has not yet been applied. */
    MEMBERSHIP_CHANGE_PENDING,

    /** Cannot enter joint consensus while already in a joint configuration. */
    ENTER_WHILE_IN_JOINT_CONSENSUS,

    /** Cannot leave joint consensus when the cluster is not in a joint configuration. */
    LEAVE_WHILE_NOT_IN_JOINT_CONSENSUS,

    /** Accepting the change would exceed the leader's uncommitted size limit. */
    EXCEEDS_UNCOMMITTED_SIZE,

    /**
     * The node is not the leader and
     * {@link consensus.protocol.policy.ProposalHandleMode#DROP} is
     * configured, so the change is not forwarded.
     */
    FORWARDING_DISABLED
}
