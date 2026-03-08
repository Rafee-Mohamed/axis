package consensus.node;

import consensus.algorithm.ReadState;
import consensus.engine.VolatileState;
import consensus.message.Message;
import consensus.storage.Payload;

import java.util.List;
import java.util.Optional;

public record Work<T extends Payload>(
        Optional<VolatileState> volatileState,
        List<Message.Peer> messages,
        Optional<PersistTask> persistTask,
        Optional<ApplyTask<T>> applyTask
) {
}
