package consensus.node;

import java.time.Duration;

public record NodeConfig(
        Duration shutdownTimeout,
        Duration workTerminationTimeout
) {
}
