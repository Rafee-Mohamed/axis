package consensus.node.task;

/**
 * A unit of work that can signal completion via a callback.
 *
 * <p>Implementations carry the data needed for a specific operation
 * (persistence or state machine application) and a completion callback
 * that delivers responses back to the engine.</p>
 *
 * @see PersistTask
 * @see ApplyTask
 */
public sealed interface CompletableTask permits PersistTask, ApplyTask {

    /**
     * Signals that the task has been completed. Delivers the associated
     * responses back to the engine.
     */
    void complete();
}
