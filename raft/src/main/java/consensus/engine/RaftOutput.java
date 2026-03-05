package consensus.engine;

import consensus.algorithm.ReadState;
import consensus.algorithm.Snapshot;
import consensus.message.Message;
import consensus.storage.Entry;

import java.util.List;
import java.util.Optional;

public record RaftOutput(
        Optional<PersistentState> persistentState,
        Optional<VolatileState> volatileState,
        Optional<CheckpointState> checkpointState,

        List<Entry> entriesToPersist,
        List<Entry> entriesToApply,
        Optional<Snapshot> snapshot,

        List<Message> messages,
        List<Message> messagesAfterPersist,
        List<ReadState> readStates,

        List<Message> responsesAfterPersist,
        List<Message> responsesAfterApply
) {}