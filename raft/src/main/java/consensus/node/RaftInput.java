package consensus.node;

import consensus.algorithm.Snapshot;
import consensus.message.Message;

public interface RaftInput {
    record Tick() implements RaftInput {}
    record Propose(byte[] data) implements RaftInput {}
    record ProposeConfChange(MembershipChangeConfig cc) implements RaftInput {}
    record Step(Message msg) implements RaftInput {}
    record Advance() implements RaftInput {}
    record ReadIndex(byte[] ctx) implements RaftInput {}
    record TransferLeader(long transferee) implements RaftInput {}
    record ForgetLeader() implements RaftInput {}
    record ReportUnreachable(long id) implements RaftInput {}
    record ReportSnapshot(long id, Snapshot status) implements RaftInput {}
    record Stop() implements RaftInput {}
}
