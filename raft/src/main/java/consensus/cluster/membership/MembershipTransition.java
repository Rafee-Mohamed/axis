package consensus.cluster.membership;

/**
 * Controls which membership change protocol is selected and how the
 * leave-joint phase is triggered.
 *
 * <p>Persisted in snapshots as part of {@link MembershipConfig} so that a
 * node restoring from a snapshot knows how to handle an in-progress joint
 * configuration.</p>
 *
 * @see MembershipChanger#executeProtocol
 */
public enum MembershipTransition {

    /**
     * Let the changer decide: use simple (single-phase) if the voter
     * symmetric difference is at most 1, otherwise use joint consensus.
     */
    AUTO,

    /**
     * Always use joint consensus, and automatically propose the leave-joint
     * entry once the enter-joint entry is committed.
     */
    JOINT_AUTO,

    /**
     * Always use joint consensus, but the application must explicitly
     * propose the leave-joint entry.
     */
    JOINT_EXPLICIT,

    /** Stable config - not in a transition. */
    NONE
}
