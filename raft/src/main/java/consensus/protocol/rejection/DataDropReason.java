package consensus.protocol.rejection;

/**
 * Why a data proposal was rejected by the protocol.
 */
public enum DataDropReason {

    /** No leader is known - cannot forward or accept the proposal. */
    NO_LEADER,

    /** The proposal contained no data entries. */
    NO_DATA,

    /** This node has been removed from the cluster configuration. */
    REMOVED_FROM_CLUSTER,

    /** A leadership transfer is in progress - the leader is not accepting new proposals. */
    LEADER_TRANSFER_IN_PROGRESS,

    /** Accepting the proposal would exceed the leader's uncommitted size limit. */
    EXCEEDS_UNCOMMITTED_SIZE,

    /**
     * The node is not the leader and
     * {@link consensus.protocol.policy.ProposalHandleMode#DROP} is
     * configured, so the proposal is not forwarded.
     */
    FORWARDING_DISABLED
}