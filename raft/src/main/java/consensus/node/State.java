package consensus.node;

/**
 * Lifecycle states of a {@link Node}.
 *
 * <pre>
 *   CREATED -> RUNNING -> SHUTTING_DOWN -> TERMINATING -> TERMINATED
 * </pre>
 */
public enum State {

    /** Initial state before {@link Node#run()} is called. */
    CREATED,

    /** The event loop is actively processing inputs and producing work. */
    RUNNING,

    /** Shutdown requested - draining in-flight work within the timeout. */
    SHUTTING_DOWN,

    /** Failing remaining futures and publishing the terminal work item. */
    TERMINATING,

    /** The node has fully stopped. */
    TERMINATED
}
