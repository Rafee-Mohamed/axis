package consensus.node;

import consensus.algorithm.ReadState;
import consensus.engine.VolatileState;
import consensus.message.Message;

import java.util.List;
import java.util.Optional;

public record Work(
        Optional<VolatileState> volatileState,
        List<Message> messages,
        List<ReadState> readStates,
        Optional<PersistTask> persistTask,
        Optional <ApplyTask> applyTask
) {
}
