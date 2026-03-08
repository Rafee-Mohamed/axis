package consensus.engine;

import consensus.algorithm.ReadState;
import consensus.algorithm.Snapshot;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;
import java.util.Queue;

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