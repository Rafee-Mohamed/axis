package consensus.node.task;

import consensus.engine.VolatileState;
import consensus.message.Message;
import consensus.storage.Payload;

import java.util.List;
import java.util.Optional;

/**
 * An item produced by the node's event loop for the application to act on.
 *
 * <p>Either a {@link Work} containing messages to send and tasks to
 * execute, or a {@link Terminated} signal indicating the node has shut
 * down.</p>
 *
 * @param <T> the application's payload type
 */
public sealed interface WorkItem<T extends Payload> {

    /**
     * Normal work produced from a single {@code advance()} cycle.
     *
     * @param volatileState  role/leader change to observe, if any
     * @param messages       peer messages to send immediately
     * @param persistTask    persistence work to execute, if any
     * @param applyTask      state machine application work to execute, if any
     * @param <T>            the application's payload type
     */
    record Work<T extends Payload>(
            Optional<VolatileState> volatileState,
            List<Message.Peer> messages,
            Optional<PersistTask> persistTask,
            Optional<ApplyTask<T>> applyTask
    ) implements WorkItem<T> {
    }

    /**
     * Terminal signal indicating the node has shut down.
     *
     * @param cause the reason for termination
     * @param <T>   the application's payload type
     */
    record Terminated<T extends Payload>(Throwable cause) implements WorkItem<T> {}
}
