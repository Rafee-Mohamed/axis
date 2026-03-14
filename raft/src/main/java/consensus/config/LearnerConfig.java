package consensus.config;

/**
 * Configuration surface required by
 * {@link consensus.protocol.role.Learner}.
 */
public interface LearnerConfig {

    /**
     * Lease timeout in ticks. If the learner does not hear from the
     * leader within this many ticks, it clears its leader reference.
     *
     * @return the lease timeout
     */
    int leaseTimeout();
}
