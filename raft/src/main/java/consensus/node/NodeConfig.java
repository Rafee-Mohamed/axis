package consensus.node;

import java.time.Duration;

/**
 * Configuration for the {@link Node} event loop.
 *
 * @param shutdownTimeout        maximum time the node waits to drain in-flight
 *                               work during graceful shutdown
 * @param workTerminationTimeout maximum time to wait when publishing the
 *                               terminal {@code WorkItem} to the outbox
 */
public record NodeConfig(
        Duration shutdownTimeout,
        Duration workTerminationTimeout
) {
}
