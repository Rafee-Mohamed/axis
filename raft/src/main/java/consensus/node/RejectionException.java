package consensus.node;

import consensus.algorithm.DataDropReason;
import consensus.algorithm.MembershipChangeDropReason;
import consensus.algorithm.ReadDropReason;
import consensus.algorithm.Rejection;

public sealed class RejectionException extends RuntimeException {

    public RejectionException(String s) {
        super(s);
    }

    static RejectionException from(Rejection r) {
        return switch (r) {
            case Rejection.DataProposalRejected dpr -> new DataProposalRejectedException(dpr.reason());
            case Rejection.MembershipChangeRejected mcr -> new MembershipChangeRejectedException(mcr.reason());
            case Rejection.ReadIndexRejected rir -> new ReadIndexRejectedException(rir.reason());
        };
    }

    public static final class DataProposalRejectedException extends RejectionException {
        private final DataDropReason reason;
        public DataProposalRejectedException(DataDropReason reason) {
            super("Data proposal rejected: " + reason);
            this.reason = reason;
        }
        public DataDropReason reason() { return reason; }
    }

    public static final class MembershipChangeRejectedException extends RejectionException {
        private final MembershipChangeDropReason reason;
        public MembershipChangeRejectedException(MembershipChangeDropReason reason) {
            super("Membership change rejected: " + reason);
            this.reason = reason;
        }
        public MembershipChangeDropReason reason() { return reason; }
    }

    public static final class ReadIndexRejectedException extends RejectionException {
        private final ReadDropReason reason;
        public ReadIndexRejectedException(ReadDropReason reason) {
            super("Read index rejected: " + reason);
            this.reason = reason;
        }
        public ReadDropReason reason() { return reason; }
    }
}