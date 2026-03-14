package consensus.engine;

import consensus.cluster.membership.MembershipChanges;
import consensus.message.Message;
import consensus.core.NodeId;
import consensus.storage.Payload;

import java.util.List;

/**
 * All possible inputs to the {@link RaftEngine}.
 *
 * <p>Each variant maps to one or more internal {@link Message} types.
 * The engine translates these into the appropriate message, routes it
 * through the Raft protocol, and produces a {@link RaftOutput}.</p>
 *
 * @see RaftEngine#process(RaftInput)
 */
public sealed interface RaftInput {

    /** Advance all timers by one tick. */
    record Tick() implements RaftInput {}

    /**
     * Propose data entries for replication.
     *
     * @param data the payloads to replicate
     */
    record ProposeData(List<? extends Payload> data) implements RaftInput {}

    /**
     * Propose a membership configuration change.
     *
     * @param changes the membership changes to apply
     */
    record ProposeMembershipChange(MembershipChanges changes) implements RaftInput {}

    /** Propose leaving joint consensus. */
    record ProposeLeaveJoint() implements RaftInput {}

    /**
     * Deliver a peer message received from the network.
     *
     * @param message the peer message
     */
    record Receive(Message.Peer message) implements RaftInput {}

    /** Request a read index for linearizable reads. */
    record ReadIndex() implements RaftInput {}

    /**
     * Report that committed entries have been applied to the state machine.
     *
     * @param response the apply completion response
     */
    record ApplyResponse(Message.AppliedToStateMachine response) implements RaftInput {}

    /**
     * Deliver persistence completion responses.
     *
     * @param responses the persistence completion responses
     */
    record PersistResponses(List<Message> responses) implements RaftInput {}

    /** Trigger an election campaign immediately. */
    record TriggerElection() implements RaftInput {}

    /**
     * Report that a peer is unreachable.
     *
     * @param id the unreachable peer
     */
    record ReportUnreachablePeer(NodeId id) implements RaftInput {}

    /**
     * Report the outcome of a snapshot delivery to a peer.
     *
     * @param id      the peer the snapshot was sent to
     * @param success {@code true} if delivered, {@code false} if failed
     */
    record ReportSnapshotStatus(NodeId id, boolean success) implements RaftInput {}

    /**
     * Request leadership transfer to a specific node.
     *
     * @param transferee the node that should become leader
     */
    record TransferLeader(NodeId transferee) implements RaftInput {}

    /** Clear the known leader reference. */
    record ForgetLeader() implements RaftInput {}

    /**
     * Apply a committed membership change to the protocol state.
     *
     * @param changes the membership changes to apply
     */
    record ApplyMembership(MembershipChanges changes) implements RaftInput {}

    /** Apply leaving joint consensus to the protocol state. */
    record ApplyLeaveJoint() implements RaftInput {}
}
