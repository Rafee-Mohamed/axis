package consensus.protocol.role;

import consensus.core.NodeId;

/**
 * A role that receives replicated entries from a leader.
 *
 * <p>Both {@link Follower} and {@link Learner} track an optional leader
 * reference, forward proposals and reads to the leader, and process
 * AppendEntries, heartbeats, and snapshots from it. This interface
 * captures that shared contract, allowing the protocol to handle
 * common forwarding logic without distinguishing between the two.</p>
 *
 * <p>The leader reference may be absent - a node can be a replicant
 * without knowing who the current leader is (e.g., after startup or
 * after a leader failure).</p>
 */
public sealed interface Replicant extends Role permits Follower, Learner {

    /**
     * Returns {@code true} if a leader is currently known.
     *
     * @return whether a leader reference is present
     */
    boolean hasLeader();

    /**
     * Returns the id of the known leader.
     *
     * @return the leader's node id
     * @throws java.util.NoSuchElementException if no leader is known
     */
    NodeId leaderId();

    /**
     * Sets or clears the leader reference.
     *
     * @param id the leader's node id, or {@code null} to clear
     */
    void setLeader(NodeId id);

    /**
     * Clears the leader reference. The node remains in its current
     * term and role - it simply no longer knows who the leader is.
     */
    void forgetLeader();
}
