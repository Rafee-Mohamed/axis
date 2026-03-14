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

/**
 * The application-facing driver for the Raft protocol.
 *
 * <p>Wraps the core {@link Raft} state machine and provides a
 * process/advance loop:</p>
 * <ol>
 *   <li>{@link #process(RaftInput)} - feed inputs (ticks, proposals,
 *       messages, responses) into the protocol.</li>
 *   <li>{@link #advance()} - collect the resulting output (state to
 *       persist, entries to apply, messages to send).</li>
 * </ol>
 *
 * <p>The engine tracks persistent, volatile, and checkpoint state diffs
 * so that {@link #advance()} only returns what has actually changed since
 * the last call.</p>
 *
 * @see RaftInput
 * @see RaftOutput
 */
public class RaftEngine {

    /** The core protocol state machine. */
    private final Raft raft;

    /** Last observed persistent state - used to detect changes. */
    private PersistentState persistentState;

    /** Last observed volatile state - used to detect changes. */
    private VolatileState volatileState;

    /** Last observed checkpoint state - used to detect changes. */
    private CheckpointState checkpointState;

    /**
     * Creates a new engine, initializing the Raft protocol from the given
     * persisted state and log storage.
     *
     * @param state      the persisted hard state (term, vote, committed index)
     * @param config     the raft configuration
     * @param logStorage the log storage implementation
     * @param membership the initial membership configuration
     * @param random     random generator for election timeout jitter
     * @throws StorageException if the log storage cannot be read
     */
    public RaftEngine(
            RaftState state,
            RaftConfig config,
            LogStorage logStorage,
            MembershipConfig membership,
            RandomGenerator random
    ) throws StorageException {
        raft = new Raft(state, config, logStorage, membership, random);
        persistentState = new PersistentState(raft.term(), raft.votedFor());
        volatileState = new VolatileState(raft.roleType(), raft.leaderId());
        checkpointState = new CheckpointState(state.committedIndex());
    }

    /**
     * Returns the new persistent state if term or votedFor has changed
     * since the last observation, updating the cached snapshot.
     *
     * @return the new state, or empty if unchanged
     */
    private Optional<PersistentState> nextPersistentState() {
        if (!persistentState.hasChanged(raft.term(), raft.votedFor())) {
            return Optional.empty();
        }
        persistentState = new PersistentState(raft.term(), raft.votedFor());
        return Optional.of(persistentState);
    }

    /**
     * Returns the new volatile state if role or leader has changed
     * since the last observation, updating the cached snapshot.
     *
     * @return the new state, or empty if unchanged
     */
    private Optional<VolatileState> nextVolatileState() {
        if (!volatileState.hasChanged(raft.roleType(), raft.leaderId())) {
            return Optional.empty();
        }
        volatileState = new VolatileState(raft.roleType(), raft.leaderId());
        return Optional.of(volatileState);
    }

    /**
     * Returns the new checkpoint state if the commit index has changed
     * since the last observation, updating the cached snapshot.
     *
     * @return the new state, or empty if unchanged
     */
    private Optional<CheckpointState> nextCheckpointState() {
        if (!checkpointState.hasChanged(raft.commitIndex())) {
            return Optional.empty();
        }
        checkpointState = new CheckpointState(raft.commitIndex());
        return Optional.of(checkpointState);
    }

    /**
     * Returns {@code true} if there is output to collect via {@link #advance()}.
     *
     * <p>Checks for pending messages, unstable log entries/snapshots,
     * committed entries ready to apply, and any state diffs.</p>
     *
     * @return {@code true} if {@link #advance()} would return output
     */
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

        if (checkpointState.hasChanged(raft.commitIndex())) {
            return true;
        }

        if (persistentState.hasChanged(raft.term(), raft.votedFor())) {
            return true;
        }

        return volatileState.hasChanged(raft.roleType(), raft.leaderId());
    }

    /**
     * Steps the protocol with a message that must not be rejected.
     *
     * @param response the message to process
     * @throws StorageException      if the log storage fails
     * @throws IllegalStateException if the message is unexpectedly rejected
     */
    public void stepNoReject(Message response) throws StorageException {
        var rejection = raft.step(response);
        if (rejection.isPresent()) {
            throw new IllegalStateException("Unexpected rejection from response: " + rejection.get());
        }
    }

    /**
     * Feeds an input into the Raft protocol.
     *
     * <p>Translates the {@link RaftInput} into the appropriate internal
     * {@link Message} and steps the protocol. Returns a rejection if the
     * input was not accepted (e.g., proposing on a non-leader).</p>
     *
     * @param input the input to process
     * @return a rejection reason if the input was not accepted, empty otherwise
     * @throws StorageException if the log storage fails
     */
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

    /**
     * Collects all pending output from the protocol since the last advance.
     *
     * <p>Returns empty if nothing has changed. Otherwise, returns an
     * {@link RaftOutput} containing state diffs, entries to persist/apply,
     * messages to send, and reads to serve.</p>
     *
     * <p>The application must:</p>
     * <ol>
     *   <li>Persist {@code persistentState}, {@code checkpointState},
     *       {@code entriesToPersist}, and {@code snapshot} to stable storage.</li>
     *   <li>Send {@code messages} to peers immediately.</li>
     *   <li>Send {@code messagesAfterPersist} only after persistence completes.</li>
     *   <li>Apply {@code committedEntriesToApply} to the state machine.</li>
     *   <li>Serve {@code readStates} to clients for linearizable reads.</li>
     *   <li>Deliver {@code persistResponses} via
     *       {@link RaftInput.PersistResponses} and {@code applyResponse} via
     *       {@link RaftInput.ApplyResponse} to close the processing loop.</li>
     * </ol>
     *
     * <p>The ordering of persist vs apply depends on the
     * {@link ExecutionModel}.</p>
     *
     * <p>Internally, this method also splits messages-after-append into
     * self-addressed responses ({@code persistResponses}) and peer-addressed
     * messages ({@code messagesAfterPersist}), and generates a
     * {@link Message.LogPersisted} response if there are unstable entries.</p>
     *
     * @return the output if there are pending changes, empty otherwise
     * @throws StorageException if the log storage fails
     */
    public Optional<RaftOutput> advance() throws StorageException  {
        if (!hasOutput()) {
            return Optional.empty();
        }

        var nextPersistentState = nextPersistentState();
        var nextVolatileState = nextVolatileState();
        var nextCheckpointState = nextCheckpointState();

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

    /**
     * Returns a point-in-time snapshot of this node's state for diagnostics.
     *
     * @return the current status
     */
    public Status status() {
        var peerStatus = raft.clusterProgress()
                .map(ClusterProgress::progress)
                .map(this::getNodeIdPeerStatusMap);

        return new Status(
                raft.id(),
                raft.term(),
                raft.votedFor(),
                raft.roleType(),
                raft.leaderId(),
                raft.commitIndex(),
                raft.appliedIndex(),
                raft.leaderTransferee(),
                raft.membership(),
                peerStatus
        );
    }

    /**
     * Converts the raw progress map to a map of {@link Status.PeerStatus}.
     *
     * @param pp the per-peer progress map
     * @return an unmodifiable map of peer status snapshots
     */
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
