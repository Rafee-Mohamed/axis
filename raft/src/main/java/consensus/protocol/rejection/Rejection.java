package consensus.protocol.rejection;

/**
 * A protocol-level rejection returned when the Raft node cannot
 * process a client request.
 *
 * <p>Each variant pairs a specific request type with its
 * corresponding drop reason enum, giving the application enough
 * context to decide how to respond (retry, redirect to leader,
 * surface an error, etc.).</p>
 */
public sealed interface Rejection {

    /**
     * A data proposal was rejected.
     *
     * @param reason why the proposal was dropped
     */
    record DataProposalRejected(DataDropReason reason) implements Rejection {}

    /**
     * A membership change proposal was rejected.
     *
     * @param reason why the membership change was dropped
     */
    record MembershipChangeRejected(MembershipChangeDropReason reason) implements Rejection {}

    /**
     * A read index request was rejected.
     *
     * @param reason why the read was dropped
     */
    record ReadIndexRejected(ReadDropReason reason) implements Rejection {}
}
