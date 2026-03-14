package consensus.protocol.policy;

/**
 * Controls how a follower initiates an election when its election timer fires.
 *
 * <p>The choice affects disruption risk: a direct election increments the
 * node's term immediately, which can force other nodes to step down even
 * if the initiator cannot win. A dual election adds a lightweight
 * pre-election round that avoids term inflation.</p>
 */
public enum ElectionProtocol {

    /**
     * Single-phase election: the follower increments its term and
     * broadcasts {@link consensus.message.Message.RequestVote} directly.
     *
     * <p>Simpler, but a partitioned or removed node will keep bumping
     * its term - when it reconnects, the higher term forces the
     * current leader to step down even though the reconnecting node
     * cannot win the election.</p>
     */
    DIRECT_ELECTION,

    /**
     * Two-phase election: the follower first broadcasts
     * {@link consensus.message.Message.RequestPreVote} at the
     * <em>next</em> term without incrementing its own term. Only if
     * a majority grants the pre-election does the node proceed to
     * a real election as {@link consensus.protocol.role.Candidate}.
     *
     * <p>This prevents term inflation from partitioned nodes -
     * a node that cannot reach a majority never increments its term,
     * so it cannot disrupt the cluster when it reconnects.</p>
     */
    DUAL_ELECTION
}
