package consensus.engine;

import consensus.algorithm.Raft;
import consensus.algorithm.RaftState;
import consensus.config.RaftConfig;
import consensus.membership.MembershipConfig;
import consensus.message.Message;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.StorageException;

import java.util.ArrayList;
import java.util.Optional;
import java.util.random.RandomGenerator;

public class RaftEngine {
    private final Raft raft;
    private PersistentState persistentState;
    private VolatileState volatileState;
    private CheckpointState checkpointState;

    public RaftEngine(
            RaftState state,
            RaftConfig config,
            LogStorage logStorage,
            MembershipConfig membership,
            RandomGenerator random
    ) throws StorageException {
        raft = new Raft(state, config, logStorage, membership, random);
        persistentState = raft.persistentState();
        volatileState = raft.volatileState();
        checkpointState = new CheckpointState(state.committedIndex());
    }

    public boolean hasOutput() {
        if (raft.hasOutput()) {
            return true;
        }

        if (raft.log().hasUnstableSnapshot()) {
            return true;
        }

        if (raft.log().hasUnstableEntries()) {
            return true;
        }

        if (raft.log().hasEntriesToPersist()) {
            return true;
        }

        if (raft.log().hasCommittedEntriesToApply()) {
            return true;
        }

        if (!checkpointState.equals(raft.checkpointState())) {
            return true;
        }

        if (!persistentState.equals(raft.persistentState())) {
            return true;
        }

        return !volatileState.equals(raft.volatileState());
    }

    public void process(RaftInput input) throws StorageException {
        switch (input) {
            case RaftInput.Tick _ -> raft.tick();
            case RaftInput.ProposeData(var data) -> raft.step(new Message.DataProposal(raft.id(), raft.id(), data));
            case RaftInput.ProposeMembershipChange(var changes) -> raft.step(new Message.MembershipChangeProposal(raft.id(), changes));
            case RaftInput.ProposeLeaveJoint _ -> raft.step(new Message.LeaveJointProposal());
            case RaftInput.Receive(var message) -> raft.step(message);
            case RaftInput.ReadIndex() -> raft.step(new Message.ReadIndex(raft.id(), raft.id()));
            case RaftInput.Responses(var responses) -> { for (var msg : responses) raft.step(msg); }
            case RaftInput.TriggerElection() -> raft.step(new Message.TriggerElection(raft.id()));
            case RaftInput.ReportUnreachablePeer(var id) -> raft.step(new Message.PeerUnreachable(id));
            case RaftInput.ReportSnapshotStatus(var id, var success) -> raft.step(new Message.SnapshotStatus(id, success));
            case RaftInput.TransferLeader(var from, var transferee) -> raft.step(new Message.TransferLeadership(raft.id(), from, transferee));
            case RaftInput.ForgetLeader() -> raft.step(new Message.ForgetLeader());
            case RaftInput.ApplyMembership(var changes) -> raft.applyMembershipChange(changes);
            case RaftInput.ApplyLeaveJoint _ -> raft.applyLeaveJoint();
        }
    }

    public Optional<RaftOutput> advance() throws StorageException  {
        if (!hasOutput()) {
            return Optional.empty();
        }

        var raftPersistentState = raft.persistentState();
        var raftVolatileState = raft.volatileState();
        var raftCheckpointState = raft.checkpointState();

        var nextPersistentState = Optional.ofNullable(raftPersistentState.equals(persistentState) ? null : raftPersistentState);
        var nextVolatileState  = Optional.ofNullable(raftVolatileState.equals(volatileState) ? null : raftVolatileState);
        var nextCheckpointState = Optional.ofNullable(raftCheckpointState.equals(checkpointState) ? null : raftCheckpointState);

        persistentState = raftPersistentState;
        volatileState = raftVolatileState;
        checkpointState = raftCheckpointState;

        var messages = raft.drainMessages();
        var messagesAfterAppend = raft.drainMessagesAfterAppend();
        var readStates = raft.drainReadStates();

        var snapshot = Optional.ofNullable(raft.log().nextUnstableSnapshot());

        var entriesToPersist = raft.log().nextEntriesToPersist();
        var entriesToApply = raft.log().nextCommittedEntries();

        var responsesAfterPersist = new ArrayList<Message>();
        var responsesAfterApply = new ArrayList<Message>();

        raft.log().acceptUnstable();
        if (!entriesToApply.isEmpty()) {
            raft.log().acceptApplying(entriesToApply.getLast().index(), Entry.calculateSize(entriesToApply));
            responsesAfterApply.add(new Message.AppliedToStateMachine(entriesToApply));
        }

        var peerMessagesAfterAppend = new ArrayList<Message>();

        for (var msg: messagesAfterAppend) {
            var msgTo = switch (msg) {
                case Message.RequestVoteResponse res -> res.to();
                case Message.AppendEntriesResponse res -> res.to();
                case Message.RequestPreVoteResponse res -> res.to();
                default -> throw new IllegalStateException("Invalid messages in messages after append");
            };

            if (raft.id().equals(msgTo)) {
                responsesAfterPersist.add(msg);
            } else {
                peerMessagesAfterAppend.add(msg);
            }
        }

        if (raft.log().hasUnstableEntries()) {
            var lastEntryToPersist = raft.log().lastEntryId();
            responsesAfterPersist.add(new Message.LogPersisted(raft.term(), lastEntryToPersist.term(), lastEntryToPersist.index(), snapshot));
        }

        return Optional.of(new RaftOutput(
                nextPersistentState,
                nextVolatileState,
                nextCheckpointState,
                entriesToPersist,
                entriesToApply,
                snapshot,
                messages,
                peerMessagesAfterAppend,
                readStates,
                responsesAfterPersist,
                responsesAfterApply
        ));
    }


}
