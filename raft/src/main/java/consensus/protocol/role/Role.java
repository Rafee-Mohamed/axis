package consensus.protocol.role;

/**
 * The current protocol role of a Raft node.
 *
 * <p>A node is always in exactly one role. The protocol drives transitions
 * between roles based on elections, term changes, and membership:</p>
 *
 * <pre>
 *   DUAL_ELECTION:
 *     Follower ---> PreCandidate ---> Candidate ---> Leader
 *              timeout          won pre-election   won election
 *
 *   DIRECT_ELECTION:
 *     Follower ---> Candidate ---> Leader
 *              timeout          won election
 *
 *   Candidate / PreCandidate ---> Follower  (higher term or election lost)
 *   Leader                   ---> Follower  (higher term or quorum loss)
 *
 *   Learner  (non-voting; no election transitions)
 * </pre>
 *
 * <p>Sealed to the five concrete roles. {@link Replicant} further groups
 * {@link Follower} and {@link Learner} - the roles that receive
 * replicated entries from a leader.</p>
 */
public sealed interface Role permits Leader, Replicant, Candidate, PreCandidate {

    /**
     * Returns the {@link RoleType} tag for this role.
     *
     * @return the role type, never {@code null}
     */
    RoleType type();
}
