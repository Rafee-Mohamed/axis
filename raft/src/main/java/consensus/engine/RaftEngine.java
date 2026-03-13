package consensus.engine;

import consensus.cluster.progress.ClusterProgress;
import consensus.protocol.*;
import consensus.protocol.rejection.Rejection;
import consensus.cluster.progress.PeerProgress;
import consensus.config.RaftConfig;
import consensus.cluster.membership.MembershipConfig;
import consensus.core.NodeId;
import consensus.core.RaftState;
import consensus.message.Message;
import consensus.storage.Entry;
import consensus.storage.LogStorage;
import consensus.storage.StorageException;

import java.util.*;
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

    public void stepNoReject(Message response) throws StorageException {
        var rejection = raft.step(response);
        if (rejection.isPresent()) {
            throw new IllegalStateException("Unexpected rejection from response: " + rejection.get());
        }
    }

    public Optional<Rejection> process(RaftInput input) throws StorageException {
        return switch (input) {
            case RaftInput.Tick _ -> raft.step(new Message.Tick());
            case RaftInput.ProposeData(var data) -> raft.step(new Message.DataProposal(raft.id(), raft.id(), data));
            case RaftInput.ProposeMembershipChange(var changes) -> raft.step(new Message.MembershipChangeProposal(raft.id(), raft.id(), changes));
            case RaftInput.ProposeLeaveJoint _ -> raft.step(new Message.LeaveJointProposal(raft.id(), raft.id()));
            case RaftInput.Receive(var message) -> raft.step(message);
            case RaftInput.ReadIndex() -> raft.step(new Message.ReadIndex(raft.id(), raft.id()));
            case RaftInput.TriggerElection() -> raft.step(new Message.TriggerElection(raft.id()));
            case RaftInput.ReportUnreachablePeer(var id) -> raft.step(new Message.PeerUnreachable(id));
            case RaftInput.ReportSnapshotStatus(var id, var success) -> raft.step(new Message.SnapshotStatus(id, success));
            case RaftInput.TransferLeader(var transferee) -> raft.step(new Message.TransferLeadership(raft.id(), raft.id(), transferee));
            case RaftInput.ForgetLeader() -> raft.step(new Message.ForgetLeader());
            case RaftInput.ApplyMembership(var changes) -> raft.step(new Message.ApplyMembershipChange(changes));
            case RaftInput.ApplyLeaveJoint _ -> raft.step(new Message.ApplyLeaveJoint());
            case RaftInput.ApplyResponse(var response) -> {
                stepNoReject(response);
                yield Optional.empty();
            }
            case RaftInput.PersistResponses(var responses) -> {
                for (var res : responses) stepNoReject(res);
                yield Optional.empty();
            }
        };
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
        var applyResponse = Optional.<Message.AppliedToStateMachine>empty();

        raft.log().acceptUnstable();
        if (!entriesToApply.isEmpty()) {
            raft.log().acceptApplying(entriesToApply.getLast().index(), Entry.calculateSize(entriesToApply));
            applyResponse = Optional.of(new Message.AppliedToStateMachine(entriesToApply));
        }

        var peerMessagesAfterAppend = new ArrayList<Message.Peer>();

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

        var readsAwaitingApply = raft.readsAwaitingApply();
        var committedEntriesAwaitingApply = raft.log().allAppliableEntries();

        return Optional.of(new RaftOutput(
                nextPersistentState,
                nextVolatileState,
                nextCheckpointState,
                entriesToPersist,
                entriesToApply,
                committedEntriesAwaitingApply,
                snapshot,
                messages,
                peerMessagesAfterAppend,
                readStates,
                readsAwaitingApply,
                responsesAfterPersist,
                applyResponse
        ));
    }

    public Status status() {
        var persistentState = raft.persistentState();
        var volatileState = raft.volatileState();
        var commitIndex = raft.commitIndex();
        var appliedIndex = raft.appliedIndex();
        var id = raft.id();
        var transferee = raft.leaderTransferee();
        var membership = raft.membership();
        var peerStatus = raft.clusterProgress()
                .map(ClusterProgress::progress)
                .map(this::getNodeIdPeerStatusMap);


        return new Status(
                id,
                persistentState.term(),
                persistentState.votedFor(),
                volatileState.role(),
                volatileState.leaderId(),
                commitIndex,
                appliedIndex,
                transferee,
                membership,
                peerStatus
        );
    }

    private Map<NodeId, Status.PeerStatus> getNodeIdPeerStatusMap(Map<NodeId, PeerProgress> pp) {
            var progressStatus = new HashMap<NodeId, Status.PeerStatus>(pp.size());
            for (var entry: pp.entrySet()) {
                var p = entry.getValue();
                progressStatus.put(
                        entry.getKey(),
                        new Status.PeerStatus(
                                p.match(),
                                p.next(),
                                p.sentCommit(),
                                p.state(),
                                p.isActive()
                        )
                );
            }

        return Collections.unmodifiableMap(progressStatus);
    }


}
