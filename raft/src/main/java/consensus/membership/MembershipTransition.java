package consensus.membership;

/**
 * Controls the behavior when leaving joint consensus during a membership change.
 *
 * <p>Membership changes in Raft use a two-phase joint consensus protocol:
 * first, the cluster enters a joint state where both the old and new configs
 * must agree (enter joint); then, the old config is dropped (leave joint).
 * This enum determines how the "leave joint" phase is triggered.</p>
 *
 * <p>This value is persisted in snapshots as part of {@link MembershipConfig},
 * so that a node restoring from a snapshot knows how to handle an in-progress
 * joint configuration.</p>
 */
public enum MembershipTransition {
    AUTO,
    /** Automatically propose the leave-joint entry once the joint entry is committed. */
    JOINT_AUTO,
    /** Application must explicitly propose the leave-joint entry. */
    JOINT_EXPLICIT,
    NONE
}
