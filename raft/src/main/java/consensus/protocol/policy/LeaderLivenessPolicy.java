package consensus.protocol.policy;

/**
 * Controls whether the leader actively verifies it still holds a
 * majority, and whether followers protect an active leader from
 * disruptive elections.
 *
 * <p>This policy has two complementary effects:</p>
 * <ul>
 *   <li><b>Leader side:</b> whether the leader periodically checks
 *       that a majority of peers are still responsive, stepping down
 *       if they are not.</li>
 *   <li><b>Follower side:</b> whether followers that have recently
 *       heard from a leader reject
 *       {@link consensus.message.Message.RequestVote} (and
 *       {@link consensus.message.Message.RequestPreVote}) messages
 *       to prevent partitioned nodes from disrupting the cluster.</li>
 * </ul>
 */
public enum LeaderLivenessPolicy {

    /**
     * No liveness verification. The leader does not check quorum, and
     * followers do not protect the leader from disruptive elections.
     *
     * <p>A partitioned leader will continue to believe it is the leader
     * until it sees a higher term. Suitable for environments where
     * network partitions are rare or handled externally.</p>
     */
    UNMONITORED,

    /**
     * Active liveness verification. The leader periodically checks
     * that a majority of peers responded since the last check; if not,
     * it steps down to follower. Followers that have recently heard
     * from the leader reject vote requests from other nodes, preventing
     * disruption from partitioned or removed nodes.
     *
     * <p>This is required for {@link ReadIndexMode#LEASE} to be safe -
     * the quorum check provides the implicit lease window that
     * guarantees no other leader can be elected while the current
     * leader believes its lease is valid.</p>
     */
    QUORUM_VERIFIED
}
