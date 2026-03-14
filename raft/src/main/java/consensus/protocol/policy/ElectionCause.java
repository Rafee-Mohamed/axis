package consensus.protocol.policy;

/**
 * Why an election was initiated - carried on
 * {@link consensus.message.Message.RequestVote} messages to control
 * whether the <b>leader lease check</b> is enforced or bypassed.
 *
 * <p>When {@link LeaderLivenessPolicy#QUORUM_VERIFIED} is active,
 * followers that have recently heard from a leader reject vote requests
 * to prevent disruption from partitioned nodes. Leadership transfers
 * need to bypass this protection because the current leader itself is
 * voluntarily giving up power.</p>
 *
 * <pre>
 *   Cause                  Lease check       Trigger
 *   ---------------------  ----------------  ----------------------------
 *   ELECTION_TIMEOUT       enforced          follower election timer fires
 *   LEADER_TRANSFER        bypassed          leader sends TimeoutNow
 *   WON_PREELECTION        enforced          pre-election quorum reached
 *   ELECTION_ROUND_TIMEOUT enforced          election round timer fires
 * </pre>
 */
public enum ElectionCause {

    /**
     * Normal election triggered by a follower's election timer expiring.
     *
     * <p>When {@link LeaderLivenessPolicy#QUORUM_VERIFIED} is active,
     * voters that have recently heard from a leader (within their
     * election timeout) will <em>reject</em> this vote to protect the
     * active leader from disruption.</p>
     */
    ELECTION_TIMEOUT,

    /**
     * Election triggered by a
     * {@link consensus.message.Message.TimeoutNow} message from the
     * current leader as part of a graceful leadership transfer.
     *
     * <p>Voters <em>bypass</em> the leader lease check and evaluate the
     * vote purely on term and log freshness. This is safe because the
     * leader itself initiated the transfer - it wants to give up
     * leadership, so the lease should not block the handoff.</p>
     *
     * <p>Transfer elections skip the pre-election phase (always use
     * {@link consensus.message.Message.RequestVote}, never
     * {@link consensus.message.Message.RequestPreVote}), because the
     * leader already verified the transferee is caught up - there is
     * no risk of a disruptive failed election.</p>
     */
    LEADER_TRANSFER,

    /**
     * Election triggered after winning a pre-election round as
     * {@link consensus.protocol.role.PreCandidate}.
     *
     * <p>The node already confirmed a majority is willing to support it
     * in a real election. This cause proceeds directly to
     * {@link consensus.message.Message.RequestVote} (never another
     * pre-election). Voters apply the same lease check as
     * {@link #ELECTION_TIMEOUT} - the pre-election win does not
     * bypass it.</p>
     */
    WON_PREELECTION,

    /**
     * Election re-triggered because a
     * {@link consensus.protocol.role.Candidate}'s or
     * {@link consensus.protocol.role.PreCandidate}'s election round
     * timer expired without reaching a decisive quorum result.
     *
     * <p>This happens when a split vote or network partition prevents a
     * majority decision within the randomized election round timeout.
     * The node restarts the election with a fresh round (and fresh
     * randomized timeout) to break the livelock. Voters apply the same
     * lease check as {@link #ELECTION_TIMEOUT}.</p>
     */
    ELECTION_ROUND_TIMEOUT
}
