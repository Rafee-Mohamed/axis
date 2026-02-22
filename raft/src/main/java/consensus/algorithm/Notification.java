package consensus.algorithm;

public sealed interface Notification {
    record DropProposal(ProposalDropReason reason) implements Notification{};
}
