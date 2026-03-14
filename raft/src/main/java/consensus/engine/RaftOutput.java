package consensus.engine;

import consensus.protocol.read.ReadState;
import consensus.core.Snapshot;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;

/**
 * The output produced by {@link RaftEngine#advance()}.
 *
 * <p>Contains everything the application needs to act on after a
 * processing cycle: state to persist, entries to apply, messages to
 * send, and reads to serve.</p>
 *
 * <p>Fields fall into four groups:</p>
 * <ul>
 *   <li><b>State diffs</b> - {@code persistentState}, {@code volatileState},
 *       {@code checkpointState}. Present only when the value changed since the
 *       last advance.</li>
 *   <li><b>Log and snapshot</b> - {@code entriesToPersist}, {@code snapshot},
 *       {@code committedEntriesToApply}, {@code committedEntriesAwaitingApply}.
 *       Entries and snapshots to write to storage or apply to the state machine.</li>
 *   <li><b>Messages and reads</b> - {@code messages}, {@code messagesAfterPersist},
 *       {@code readStates}, {@code readsAwaitingApply}.
 *       Peer messages to send and linearizable reads to serve.</li>
 *   <li><b>Responses</b> - {@code persistResponses}, {@code applyResponse}.
 *       Messages that must be delivered back to the engine after persistence
 *       or apply completes, closing the processing loop.</li>
 * </ul>
 *
 * @param persistentState           hard state change (term/vote) to write to
 *                                  stable storage. Must survive crashes.
 * @param volatileState             role/leader change to observe. Not persisted -
 *                                  rebuilt from the protocol on recovery.
 * @param checkpointState           committed index change to persist. A performance
 *                                  optimization - avoids re-applying entries after restart.
 * @param entriesToPersist          new log entries from the unstable log to write
 *                                  to stable storage
 * @param committedEntriesToApply   committed entries released for state machine
 *                                  application in this cycle, bounded by the
 *                                  apply backpressure limit
 * @param committedEntriesAwaitingApply committed entries not yet released for apply -
 *                                  the overflow beyond {@code committedEntriesToApply}
 *                                  due to backpressure. Useful for tracking proposals
 *                                  that are committed but awaiting application.
 * @param snapshot                  incoming snapshot from the leader to persist, if any
 * @param messages                  peer messages to send immediately
 * @param messagesAfterPersist      peer messages that must only be sent after
 *                                  persistence of entries and hard state completes
 * @param readStates                read indices that are confirmed and applied -
 *                                  ready for the application to serve
 * @param readsAwaitingApply        read indices confirmed by the leader but not yet
 *                                  released (local applied index has not caught up).
 *                                  Useful for tracking read progress.
 * @param persistResponses          responses to deliver via {@link RaftEngine#process}
 *                                  after persistence completes
 * @param applyResponse             response to deliver via {@link RaftEngine#process}
 *                                  after entries are applied
 */
public record RaftOutput(
        Optional<PersistentState> persistentState,
        Optional<VolatileState> volatileState,
        Optional<CheckpointState> checkpointState,

        List<Entry> entriesToPersist,
        List<Entry> committedEntriesToApply,
        List<Entry> committedEntriesAwaitingApply,
        Optional<Snapshot> snapshot,

        List<Message.Peer> messages,
        List<Message.Peer> messagesAfterPersist,
        List<ReadState> readStates,
        List<ReadState> readsAwaitingApply,

        List<Message> persistResponses,
        Optional<Message.AppliedToStateMachine> applyResponse
) {}
