package consensus.protocol.policy;

/**
 * Controls how a non-leader node handles client proposals and
 * membership change requests.
 *
 * <p>Only the leader can append entries to the log. When a follower
 * or learner receives a proposal, this policy determines whether
 * it is forwarded to the known leader or silently dropped.</p>
 */
public enum ProposalHandleMode {

    /**
     * Forward the proposal to the current leader (if known). If no
     * leader is known, the proposal is rejected with a
     * {@link consensus.protocol.rejection.Rejection}.
     */
    FORWARD_TO_LEADER,

    /**
     * Drop the proposal and reject it immediately. The application
     * is responsible for routing proposals to the leader itself.
     *
     * <p>Useful when the application layer has its own leader-routing
     * mechanism (e.g. a load balancer or client-side discovery) and
     * does not want the Raft layer to forward messages on its behalf.</p>
     */
    DROP
}
