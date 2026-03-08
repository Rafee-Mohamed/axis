package consensus.node;

public sealed class NodeLifecycleException extends Exception permits NodeLifecycleException.GracefulShutdown, NodeLifecycleException.Termination, NodeLifecycleException.ShuttingDown {

    public NodeLifecycleException(String message) { super(message); }
    public NodeLifecycleException(String message, Throwable cause) { super(message, cause); }

    // Node completed graceful shutdown
    public static final class GracefulShutdown extends NodeLifecycleException {
        public GracefulShutdown() { super("Node completed graceful shutdown"); }
    }

    // Node terminated due to fatal error (wraps StorageException)
    public static final class Termination extends NodeLifecycleException {
        public Termination(Throwable cause) { super("Node terminated due to fatal error", cause); }
    }

    // Node is in the process of shutting down (used for futures, not thrown by run())
    public static final class ShuttingDown extends NodeLifecycleException {
        public ShuttingDown() { super("Node is shutting down, operation rejected"); }
    }
}
