package consensus.node;

import consensus.engine.VolatileState;
import consensus.message.Message;
import consensus.storage.Payload;

import java.util.List;
import java.util.Optional;

public sealed interface WorkItem<T extends Payload> {
    record Work<T extends Payload>(
            Optional<VolatileState> volatileState,
            List<Message.Peer> messages,
            Optional<PersistTask> persistTask,
            Optional<ApplyTask<T>> applyTask
    ) implements WorkItem<T> {
    }

    record Terminated<T extends Payload>(Throwable cause) implements WorkItem<T> {}
}
