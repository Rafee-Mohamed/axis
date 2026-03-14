package consensus.node;

/**
 * Exceptions representing node lifecycle events.
 *
 * <p>Used both as control flow signals from {@link Node#run()} and
 * as failure causes for in-flight futures.</p>
 */
public sealed class NodeLifecycleException extends Exception permits NodeLifecycleException.GracefulShutdown, NodeLifecycleException.Termination, NodeLifecycleException.ShuttingDown {

    /** @param message the detail message */
    public NodeLifecycleException(String message) { super(message); }

    /**
     * @param message the detail message
     * @param cause   the underlying cause
     */
    public NodeLifecycleException(String message, Throwable cause) { super(message, cause); }

    /** The node completed graceful shutdown - no error occurred. */
    public static final class GracefulShutdown extends NodeLifecycleException {
        public GracefulShutdown() { super("Node completed graceful shutdown"); }
    }

    /** The node terminated due to a fatal error (e.g., storage failure). */
    public static final class Termination extends NodeLifecycleException {
        public Termination(Throwable cause) { super("Node terminated due to fatal error", cause); }
    }

    /**
     * The node is in the process of shutting down. Used to fail futures
     * for operations submitted after shutdown begins - not thrown by
     * {@link Node#run()}.
     */
    public static final class ShuttingDown extends NodeLifecycleException {
        public ShuttingDown() { super("Node is shutting down, operation rejected"); }
    }
}
