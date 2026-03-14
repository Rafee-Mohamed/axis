package consensus.message;

import consensus.protocol.policy.ElectionCause;
import consensus.core.Snapshot;
import consensus.cluster.membership.MembershipChanges;
import consensus.storage.Entry;
import consensus.core.NodeId;
import consensus.storage.Payload;

import java.util.List;
import java.util.Optional;

/**
 * All messages in the Raft protocol - both peer messages sent between
 * nodes over the network and internal messages that stay local to a
 * single node.
 *
 * <p>Every peer message carries {@code to}, {@code from}, and
 * {@code term}. Internal messages omit routing fields since they
 * never leave the node.</p>
 */
public sealed interface Message {

    /**
     * A message that stays local to the node - never sent over the
     * network. Used for ticks, persistence feedback, state machine
     * feedback, and operator commands.
     */
    sealed interface Internal extends Message {}

    /**
     * A message exchanged between nodes over the network. Carries
     * at least {@code to} and {@code from} fields for routing.
     */
    sealed interface Peer extends Message {}

    /* ==================== REPLICATION ==================== */

    /**
     * Leader -> Follower/Learner: replicate log entries.
     *
     * <p>The leader sends a batch of entries along with the entry
     * immediately before them ({@code prevLogIndex}/{@code prevLogTerm})
     * so the receiver can verify log consistency before appending. Also
     * carries the leader's commit index so followers can advance their
     * own.</p>
     *
     * <p>When {@code entries} is empty, this acts as a probe - the
     * leader sends it to deliver its commit index, or to unblock a
     * stalled peer whose inflight buffer is full.</p>
     *
     * @param to            the target peer
     * @param from          the leader sending this message
     * @param term          the leader's current term
     * @param prevLogTerm   term of the entry at prevLogIndex
     * @param prevLogIndex  index of the entry immediately before the
     *                      new ones
     * @param entries       log entries to append (empty for probe)
     * @param leaderCommit  the leader's commit index - receiver advances
     *                      its own commit to
     *                      {@code min(leaderCommit, lastNewEntryIndex)}
     */
    record AppendEntries(
            NodeId to,
            NodeId from,
            long term,
            long prevLogTerm,
            long prevLogIndex,
            List<Entry> entries,
            long leaderCommit
    ) implements Peer {
    }

    /**
     * Follower/Learner -> Leader: response to
     * {@link AppendEntries} or {@link InstallSnapshot}.
     *
     * <p>This message serves double duty - it is the response for both
     * AppendEntries and InstallSnapshot. The fields have different
     * meanings depending on the outcome:</p>
     *
     * <p><b>On success</b> ({@code success = true}):</p>
     * <ul>
     *   <li>{@code index}: the highest log index known to be replicated
     *       on this follower. For AppendEntries this is the index of the
     *       last appended entry. For InstallSnapshot this is either
     *       {@code lastIndex()} (snapshot applied) or {@code committed}
     *       (snapshot was stale/redundant). The leader uses this to
     *       advance the peer's match index.</li>
     *   <li>{@code termHint} and {@code indexHint}: unused (zero).</li>
     * </ul>
     *
     * <p><b>On rejection</b> ({@code success = false}):</p>
     * <ul>
     *   <li>{@code index}: the prevLogIndex that was rejected - tells
     *       the leader which AppendEntries failed.</li>
     *   <li>{@code termHint}: the term of the follower's entry at the
     *       hint index. The leader uses this together with indexHint to
     *       skip backward efficiently.</li>
     *   <li>{@code indexHint}: the highest index where the follower has
     *       a term &lt;= the leader's prevLogTerm. Lets the leader jump
     *       backward past entire term ranges instead of probing one
     *       index at a time.</li>
     * </ul>
     *
     * @param to        the leader that sent the original request
     * @param from      the follower/learner responding
     * @param term      the responder's current term (if higher than the
     *                  leader's, the leader steps down)
     * @param success   true if log consistency check passed, false if
     *                  logs diverged
     * @param index     replicated index (success) or rejected
     *                  prevLogIndex (failure)
     * @param termHint  follower's conflicting term for log divergence
     *                  optimization
     * @param indexHint follower's suggested backtrack index for log
     *                  divergence optimization
     */
    record AppendEntriesResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean success,
            long index,
            long termHint,
            long indexHint
    ) implements Peer {
    }

    /**
     * Leader -> Follower/Learner: install a snapshot.
     *
     * <p>Sent when a peer is too far behind for log-based replication -
     * the entries it needs have been compacted away. The snapshot
     * contains the full state machine data, the last included entry's
     * term/index, and the cluster membership at that point.</p>
     *
     * <p>The receiver responds with an {@link AppendEntriesResponse}
     * (not a separate message type) - success with its new last index
     * if restored, or success with its committed index if the snapshot
     * was redundant.</p>
     *
     * @param to       the target peer
     * @param from     the leader
     * @param term     the leader's current term
     * @param snapshot the full snapshot (state machine data + metadata
     *                 + membership)
     */
    record InstallSnapshot(
            NodeId to,
            NodeId from,
            long term,
            Snapshot snapshot
    ) implements Peer {
    }

    /* ==================== HEARTBEAT ==================== */

    /**
     * Leader -> Follower/Learner: lightweight heartbeat (no entries).
     *
     * <p>Carries the leader's commit index and a sequence number for
     * linearizable read tracking. Resets the follower's election timer
     * and allows it to advance its commit index. Separate from
     * {@link AppendEntries} to keep heartbeats cheap - the leader
     * sends these at a faster cadence than full replication rounds.</p>
     *
     * @param to           the target peer
     * @param from         the leader
     * @param term         the leader's current term
     * @param leaderCommit the leader's commit index
     * @param sequence     heartbeat sequence number for read index
     *                     tracking - echoed back in
     *                     {@link HeartbeatResponse#sequence}
     */
    record Heartbeat(
            NodeId to,
            NodeId from,
            long term,
            long leaderCommit,
            long sequence
    ) implements Peer {
    }

    /**
     * Follower/Learner -> Leader: response to {@link Heartbeat}.
     *
     * <p>Confirms the follower is alive. The leader uses these to
     * track which peers are active for quorum checks. Also triggers
     * the leader to resume sending to peers stuck in Probe state.
     * The echoed {@code sequence} is used by
     * {@link consensus.protocol.read.ReadIndex} to correlate acks
     * to specific heartbeat rounds for linearizable reads.</p>
     *
     * @param to       the leader
     * @param from     the follower/learner responding
     * @param term     the responder's current term
     * @param sequence the heartbeat sequence echoed from the original
     *                 {@link Heartbeat}
     */
    record HeartbeatResponse(
            NodeId to,
            NodeId from,
            long term,
            long sequence
    ) implements Peer {
    }

    /* ==================== ELECTION ==================== */

    /**
     * Candidate -> All voters: request a vote.
     *
     * <p>Carries the candidate's last log entry (term + index) so voters
     * can enforce the "at least as up-to-date" rule - a voter only
     * grants its vote if the candidate's log is not behind its own.
     * This prevents electing a leader that would need to overwrite
     * committed entries.</p>
     *
     * <p>The {@code cause} field distinguishes <b>why</b> this election
     * is happening, which affects how voters handle the leader lease
     * check. See {@link ElectionCause} for details.</p>
     *
     * @param to           the voter being asked
     * @param from         the candidate requesting the vote
     * @param term         the candidate's term for this election
     * @param lastLogTerm  term of the candidate's last log entry
     * @param lastLogIndex index of the candidate's last log entry
     * @param cause        why this election was initiated - controls
     *                     whether the leader lease check is enforced
     *                     or bypassed
     */
    record RequestVote(
            NodeId to,
            NodeId from,
            long term,
            long lastLogTerm,
            long lastLogIndex,
            ElectionCause cause
    ) implements Peer {
    }

    /**
     * Voter -> Candidate: response to {@link RequestVote}.
     *
     * @param to           the candidate that requested the vote
     * @param from         the voter responding
     * @param term         the voter's current term (may be higher if the
     *                     candidate's term is stale)
     * @param voteGranted  true if the vote was granted
     */
    record RequestVoteResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean voteGranted
    ) implements Peer {
    }

    /**
     * PreCandidate -> All voters: pre-election request before committing
     * to a real election.
     *
     * <p>Identical to {@link RequestVote} except the term is
     * hypothetical ({@code currentTerm + 1}) - it is not adopted by
     * voters. This prevents a partitioned node from inflating
     * cluster-wide terms when it cannot actually win. Only if a quorum
     * grants the pre-election does the node proceed to a real election
     * as {@link consensus.protocol.role.Candidate}.</p>
     *
     * <p>Only used when
     * {@link consensus.protocol.policy.ElectionProtocol#DUAL_ELECTION}
     * is configured.</p>
     *
     * @param to           the voter being asked
     * @param from         the pre-candidate
     * @param term         the hypothetical term (currentTerm + 1, not
     *                     yet adopted)
     * @param lastLogTerm  term of the pre-candidate's last log entry
     * @param lastLogIndex index of the pre-candidate's last log entry
     */
    record RequestPreVote(
            NodeId to,
            NodeId from,
            long term,
            long lastLogTerm,
            long lastLogIndex
    ) implements Peer {
    }

    /**
     * Voter -> PreCandidate: response to {@link RequestPreVote}.
     *
     * @param to           the pre-candidate
     * @param from         the voter responding
     * @param term         the voter's current term
     * @param voteGranted  true if the pre-election vote was granted
     */
    record RequestPreVoteResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean voteGranted
    ) implements Peer {
    }

    /* ==================== LEADERSHIP TRANSFER ==================== */

    /**
     * Any node -> Leader: request to transfer leadership to a specific
     * node.
     *
     * <p>The leader catches up the transferee's log, then sends it a
     * {@link TimeoutNow} to trigger an immediate election. During
     * transfer the leader stops accepting proposals to let the
     * transferee catch up.</p>
     *
     * @param to         the current leader
     * @param from       the node requesting the transfer
     * @param transferee the node that should become the new leader
     */
    record TransferLeadership(
            NodeId to,
            NodeId from,
            NodeId transferee
    ) implements Peer {
    }

    /**
     * Leader -> Transferee: start an election immediately.
     *
     * <p>Sent once the leader confirms the transferee's log is caught
     * up. The transferee skips the election timeout and campaigns right
     * away. Uses a direct election (not a pre-election) since the
     * leader already verified the transferee is caught up - there is
     * no risk of a disruptive failed election.</p>
     *
     * @param to   the transferee that should start the election
     * @param from the current leader
     * @param term the leader's current term
     */
    record TimeoutNow(
            NodeId to,
            NodeId from,
            long term
    ) implements Peer {
    }

    /* ==================== LINEARIZABLE READS ==================== */

    /**
     * Follower -> Leader: request a read index for linearizable reads.
     *
     * <p>The leader responds with its current commit index after
     * confirming it still holds leadership (via a quorum heartbeat
     * ack). The follower can then serve reads once it has applied
     * entries up to that index.</p>
     *
     * @param to   the leader
     * @param from the follower requesting the read
     */
    record ReadIndex(
            NodeId to,
            NodeId from
    ) implements Peer {
    }

    /**
     * Leader -> Follower: confirmed read index for a linearizable read.
     *
     * <p>The follower can serve a linearizable read once its applied
     * index reaches {@code readIndex}.</p>
     *
     * @param to        the follower that requested the read
     * @param from      the leader
     * @param term      the leader's term
     * @param readIndex the commit index at the time the leader confirmed
     *                  its leadership - reads at or below this index are
     *                  safe
     */
    record ReadIndexResponse(
            NodeId to,
            NodeId from,
            long term,
            long readIndex
    ) implements Peer {
    }

    /* ==================== CLIENT PROPOSALS (may be forwarded to leader) ==================== */

    /**
     * Application proposes data to be replicated.
     *
     * <p>On a leader, the data is appended to the log and replicated.
     * On a follower/learner, the proposal is forwarded to the leader
     * (if {@link consensus.protocol.policy.ProposalHandleMode#FORWARD_TO_LEADER}
     * is configured) or rejected.</p>
     *
     * @param to   this node's id (self-addressed when local, leader id
     *             when forwarded)
     * @param from this node's id
     * @param data the proposed data entries
     */
    record DataProposal(
            NodeId to,
            NodeId from,
            List<? extends Payload> data
    ) implements Peer {
    }

    /**
     * Application proposes a membership configuration change (enter
     * joint consensus).
     *
     * @param to                this node's id (self-addressed when local,
     *                          leader id when forwarded)
     * @param from              this node's id
     * @param membershipChanges the membership changes to apply
     *                          (add/remove voters/learners)
     */
    record MembershipChangeProposal(
            NodeId to,
            NodeId from,
            MembershipChanges membershipChanges
    ) implements Peer {
    }

    /**
     * Application proposes to leave joint consensus and finalize the
     * membership change.
     *
     * @param to   this node's id (self-addressed when local, leader id
     *             when forwarded)
     * @param from this node's id
     */
    record LeaveJointProposal(
            NodeId to,
            NodeId from
    ) implements Peer {}

    /* ==================== INTERNAL TRIGGERS (local, never sent over network) ==================== */

    /**
     * Tick signal - advances all role timers by one tick. Delivered
     * periodically by the application's ticker.
     */
    record Tick() implements Internal {}

    /**
     * Follower's election timeout fired - start an election campaign.
     *
     * @param from this node's id
     */
    record TriggerElection(
            NodeId from
    ) implements Internal {
    }

    /**
     * Leader's heartbeat timer fired - broadcast heartbeats to all
     * peers.
     *
     * @param from this node's id (the leader)
     */
    record TriggerHeartbeat(
            NodeId from
    ) implements Internal {
    }

    /**
     * Leader's quorum-check timer fired - verify a quorum of peers
     * have been recently active, step down if not.
     *
     * <p>Only generated when
     * {@link consensus.protocol.policy.LeaderLivenessPolicy#QUORUM_VERIFIED}
     * is active.</p>
     *
     * @param from this node's id (the leader)
     */
    record CheckQuorum(
            NodeId from
    ) implements Internal {
    }

    /**
     * Instructs a follower/learner to forget its current leader.
     *
     * <p>Useful when
     * {@link consensus.protocol.policy.ElectionProtocol#DUAL_ELECTION}
     * and
     * {@link consensus.protocol.policy.LeaderLivenessPolicy#QUORUM_VERIFIED}
     * are both active: followers normally reject pre-election requests
     * if they have recently heard from the leader (lease protection).
     * ForgetLeader clears that state so the node can grant votes
     * immediately, enabling fast leader election when an external
     * system (e.g. orchestrator) knows the leader is dead.</p>
     *
     * <p>Incompatible with
     * {@link consensus.protocol.policy.ReadIndexMode#LEASE} - see
     * {@link consensus.protocol.Raft} forgetLeader handling for
     * details.</p>
     */
    record ForgetLeader() implements Internal {}

    /* ==================== FEEDBACK (from application/transport layer) ==================== */

    /**
     * Transport layer reports the outcome of a snapshot delivery.
     *
     * <p>Sent by the application/transport layer on the same node that
     * initiated the {@link InstallSnapshot}. While the snapshot is in
     * flight the peer is in Snapshot state and all AppendEntries are
     * paused. This feedback unblocks the peer by transitioning it back
     * to Probe.</p>
     *
     * @param peer    the peer the snapshot was sent to
     * @param success true if delivered, false if delivery failed
     */
    record SnapshotStatus(
            NodeId peer,
            boolean success
    ) implements Internal {
    }

    /**
     * Transport layer reports a peer is unreachable.
     *
     * <p>Sent when the transport fails to deliver a message (connection
     * refused, timeout, etc.). If the peer was in Replicate state it is
     * demoted to Probe since the pipelined messages were likely lost.
     * Probe and Snapshot states are unaffected.</p>
     *
     * @param peer the peer that could not be reached
     */
    record PeerUnreachable(
            NodeId peer
    ) implements Internal {
    }

    /**
     * Log entries, hard state, and/or snapshot have been persisted to
     * stable storage.
     *
     * <p>Entry stability is <b>term-gated</b>: only applied when
     * {@code term == currentTerm}. If the term changed the entries may
     * have been overwritten by a new leader. Snapshot application is
     * <b>not term-gated</b> - snapshots are committed state, immutable
     * across terms.</p>
     *
     * @param term     the Raft term when the persist was requested
     * @param logTerm  term of the last persisted entry
     * @param logIndex last persisted entry index (0 if no entries)
     * @param snapshot the persisted snapshot, if any
     */
    record LogPersisted(
            long term,
            long logTerm,
            long logIndex,
            Optional<Snapshot> snapshot
    ) implements Internal {}

    /**
     * Committed entries have been applied to the state machine.
     *
     * <p>Entries are echoed back so Raft can extract the last applied
     * index and total size. No term field - committed entries are
     * term-independent, permanent regardless of leadership changes.</p>
     *
     * @param entries the committed entries that were applied
     */
    record AppliedToStateMachine(
            List<Entry> entries
    ) implements Internal {
    }

    /**
     * Application requests a membership change to be applied to the
     * protocol state (called after the membership change entry is
     * committed).
     *
     * @param changes the membership changes to apply
     */
    record ApplyMembershipChange(
            MembershipChanges changes
    ) implements Internal {}

    /**
     * Application requests leaving joint consensus (called after the
     * leave-joint entry is committed).
     */
    record ApplyLeaveJoint() implements Internal {}

}
