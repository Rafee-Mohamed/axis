package consensus.config;

/**
 * Configuration surface required by
 * {@link consensus.protocol.role.Follower}.
 */
public interface FollowerConfig {

    /**
     * Base election timeout in ticks. The actual timeout used by a
     * follower is randomized in {@code [electionTimeout, 2 * electionTimeout)}
     * to reduce split-vote probability.
     *
     * @return the base election timeout
     */
    int electionTimeout();
}
