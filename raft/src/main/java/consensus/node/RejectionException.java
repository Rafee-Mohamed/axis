package consensus.node;

import consensus.protocol.rejection.DataDropReason;
import consensus.protocol.rejection.MembershipChangeDropReason;
import consensus.protocol.rejection.ReadDropReason;
import consensus.protocol.rejection.Rejection;

/**
 * Thrown when the Raft protocol rejects an input. Wraps the protocol-level
 * {@link Rejection} into a typed exception for future-based APIs.
 *
 * @see Rejection
 */
public sealed class RejectionException extends RuntimeException {

    /**
     * @param s the rejection detail message
     */
    public RejectionException(String s) {
        super(s);
    }

    /**
     * Converts a protocol-level rejection into the appropriate exception.
     *
     * @param r the rejection
     * @return the corresponding exception
     */
    static RejectionException from(Rejection r) {
        return switch (r) {
            case Rejection.DataProposalRejected dpr -> new DataProposalRejectedException(dpr.reason());
            case Rejection.MembershipChangeRejected mcr -> new MembershipChangeRejectedException(mcr.reason());
            case Rejection.ReadIndexRejected rir -> new ReadIndexRejectedException(rir.reason());
        };
    }

    /**
     * A data proposal was rejected.
     *
     * @see DataDropReason
     */
    public static final class DataProposalRejectedException extends RejectionException {
        private final DataDropReason reason;
        /** @param reason the reason the proposal was rejected */
        public DataProposalRejectedException(DataDropReason reason) {
            super("Data proposal rejected: " + reason);
            this.reason = reason;
        }
        /** @return the reason the proposal was rejected */
        public DataDropReason reason() { return reason; }
    }

    /**
     * A membership change was rejected.
     *
     * @see MembershipChangeDropReason
     */
    public static final class MembershipChangeRejectedException extends RejectionException {
        private final MembershipChangeDropReason reason;
        /** @param reason the reason the change was rejected */
        public MembershipChangeRejectedException(MembershipChangeDropReason reason) {
            super("Membership change rejected: " + reason);
            this.reason = reason;
        }
        /** @return the reason the change was rejected */
        public MembershipChangeDropReason reason() { return reason; }
    }

    /**
     * A read index request was rejected.
     *
     * @see ReadDropReason
     */
    public static final class ReadIndexRejectedException extends RejectionException {
        private final ReadDropReason reason;
        /** @param reason the reason the read was rejected */
        public ReadIndexRejectedException(ReadDropReason reason) {
            super("Read index rejected: " + reason);
            this.reason = reason;
        }
        /** @return the reason the read was rejected */
        public ReadDropReason reason() { return reason; }
    }
}
