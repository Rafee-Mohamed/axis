package consensus.engine;

import consensus.membership.MembershipChanges;
import consensus.membership.MembershipConfig;
import consensus.message.Message;
import consensus.algorithm.NodeId;

import java.util.List;

public sealed interface RaftInput {
    record Tick() implements RaftInput {}
    record ProposeData(List<byte[]> data) implements RaftInput {}
    record ProposeMembershipChange(NodeId from, MembershipChanges changes) implements RaftInput {}
    record ProposeLeaveJoint() implements RaftInput {}
    record Receive(Message message) implements RaftInput {}
    record ReadIndex() implements RaftInput {}
    record Responses(List<Message> responses) implements RaftInput {}
    record TriggerElection() implements RaftInput {}
    record ReportUnreachablePeer(NodeId id) implements RaftInput {}
    record ReportSnapshotStatus(NodeId id, boolean success) implements RaftInput {}
    record TransferLeader(NodeId from, NodeId transferee) implements RaftInput {}
    record ForgetLeader() implements RaftInput {}
    record ApplyMembership(MembershipChanges changes) implements RaftInput {}
    record ApplyLeaveJoint() implements RaftInput {}
}
