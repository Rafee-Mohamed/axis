package consensus.storage;

/**
 * Controls which committed entries are eligible for state machine
 * application, based on the chosen {@link consensus.engine.ExecutionModel}.
 *
 * @see consensus.engine.ExecutionModel
 */
public enum AppliableEntriesPolicy {

    /**
     * Entries must be both committed and persisted locally before they
     * are eligible for application. The appliable range is capped at
     * the persistence boundary.
     */
    PERSISTED_COMMITTED,

    /**
     * Entries are eligible for application as soon as they are committed,
     * even if not yet persisted locally.
     */
    COMMITTED
}
