package consensus.algorithm;

/**
 * Why an election was initiated — carried on {@code RequestVote} messages to
 * control whether the <b>leader lease check</b> is enforced or bypassed.
 *
 * <p>With {@code checkQuorum} enabled, followers that have recently heard from a
 * leader reject vote requests to prevent disruption from partitioned nodes.
 * Leadership transfers need to bypass this protection because the current leader
 * itself is voluntarily giving up power.</p>
 *
 * <pre>
 *   Follower election timeout fires
 *         │
 *         ▼
 *   cause = ELECTION_TIMEOUT
 *   RequestVote sent to all voters
 *         │
 *         ▼
 *   Voter: "Have I heard from a leader recently?"
 *     YES → reject (leader is alive, don't disrupt)
 *     NO  → evaluate log freshness and grant/reject
 *
 *   ─────────────────────────────────────────────────
 *
 *   Leader sends TimeoutNow to transferee
 *         │
 *         ▼
 *   cause = LEADER_TRANSFER
 *   RequestVote sent to all voters
 *         │
 *         ▼
 *   Voter: lease check BYPASSED (leader initiated this)
 *     → evaluate log freshness and grant/reject
 * </pre>
 */
public enum ElectionCause {

    /**
     * Normal election triggered by a follower's election timer expiring.
     *
     * <p>Voters that have recently heard from a leader (within their election
     * timeout) will <em>reject</em> this vote to protect the active leader
     * from disruption. This is the lease check that prevents partitioned or
     * removed nodes from disrupting a healthy cluster.</p>
     */
    ELECTION_TIMEOUT,

    /**
     * Election triggered by a {@code TimeoutNow} message from the current
     * leader as part of a graceful leadership transfer.
     *
     * <p>Voters <em>bypass</em> the leader lease check and evaluate the vote
     * purely on term and log freshness. This is safe because the leader itself
     * initiated the transfer — it wants to give up leadership, so the lease
     * should not block the handoff.</p>
     *
     * <p>Transfer elections also skip the pre-vote phase (always use real
     * {@code RequestVote}, never {@code RequestPreVote}), because the leader
     * already verified the transferee is caught up — there is no risk of a
     * disruptive failed election.</p>
     */
    LEADER_TRANSFER,

    /**
     * Election triggered after winning a pre-vote round.
     *
     * <p>The node already confirmed a majority is willing to support it in a
     * real election. This cause proceeds directly to {@code RequestVote}
     * (never another pre-vote). Voters apply the same lease check as
     * {@link #ELECTION_TIMEOUT} — the pre-vote win does not bypass it.</p>
     */
    WON_PREELECTION,

    /**
     * Election re-triggered because a Candidate or PreCandidate's election
     * round timer expired without reaching a decisive vote result.
     *
     * <p>This happens when a split vote or network partition prevents a
     * majority decision within the randomized election round timeout. The
     * node restarts the election with a fresh round (and fresh randomized
     * timeout) to break the livelock. Voters apply the same lease check as
     * {@link #ELECTION_TIMEOUT}.</p>
     */
    ELECTION_ROUND_TIMEOUT
}
