package consensus.engine;

import consensus.membership.MembershipChanges;
import consensus.message.Message;
import consensus.algorithm.NodeId;
import consensus.storage.Payload;

import java.util.List;
import java.util.Optional;

public sealed interface RaftInput {
    record Tick() implements RaftInput {}
    record ProposeData(List<? extends Payload> data) implements RaftInput {}
    record ProposeMembershipChange(MembershipChanges changes) implements RaftInput {}
    record ProposeLeaveJoint() implements RaftInput {}
    record Receive(Message.Peer message) implements RaftInput {}
    record ReadIndex() implements RaftInput {}
    record ApplyResponse(Message.AppliedToStateMachine response) implements RaftInput {}
    record PersistResponses(List<Message> responses) implements RaftInput {}
    record TriggerElection() implements RaftInput {}
    record ReportUnreachablePeer(NodeId id) implements RaftInput {}
    record ReportSnapshotStatus(NodeId id, boolean success) implements RaftInput {}
    record TransferLeader(NodeId transferee) implements RaftInput {}
    record ForgetLeader() implements RaftInput {}
    record ApplyMembership(MembershipChanges changes) implements RaftInput {}
    record ApplyLeaveJoint() implements RaftInput {}
}
