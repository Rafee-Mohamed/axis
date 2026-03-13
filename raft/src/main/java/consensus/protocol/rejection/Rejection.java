package consensus.protocol.rejection;

public sealed interface Rejection {
    record DataProposalRejected(DataDropReason reason) implements Rejection {}
    record MembershipChangeRejected(MembershipChangeDropReason reason) implements Rejection {}
    record ReadIndexRejected(ReadDropReason reason) implements Rejection {}
}
