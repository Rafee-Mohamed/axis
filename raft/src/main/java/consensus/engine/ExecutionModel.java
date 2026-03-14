package consensus.engine;

/**
 * Controls the concurrency between log persistence and state machine
 * application. Determines which committed entries are eligible for
 * apply and whether persist and apply can overlap.
 *
 * @see RaftOutput
 */
public enum ExecutionModel {

    /**
     * All committed entries are eligible for application regardless of
     * local persistence state - the apply step does not wait for the
     * persist step to complete.
     *
     * <p>Simpler to reason about. The application processes one
     * {@link RaftOutput} at a time: persist entries, send messages,
     * apply committed entries, then deliver responses.</p>
     *
     * <p>Maps to {@link consensus.storage.AppliableEntriesPolicy#COMMITTED}.</p>
     */
    SEQUENTIAL,

    /**
     * Only committed entries that have been persisted locally are
     * eligible for application. In exchange, persistence (log append)
     * and application (state machine apply) can execute concurrently,
     * pipelining multiple {@link RaftOutput} cycles.
     *
     * <p>Log appends and state machine applies are dispatched
     * concurrently, reducing interference between proposals and
     * increasing batching. The result is lower end-to-end commit
     * latency and higher throughput under concurrent proposals.</p>
     *
     * <p>Requires the application to handle
     * {@link RaftOutput#messagesAfterPersist()} correctly - those
     * messages must only be delivered after the corresponding
     * persistence completes.</p>
     *
     * <p>Maps to {@link consensus.storage.AppliableEntriesPolicy#PERSISTED_COMMITTED}.</p>
     */
    PIPELINED
}
