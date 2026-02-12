package consensus.node;

public record PersistentState(long term, long commit, NodeId votedFor) {
    public PersistentState() {
        this(0, 0, null);
    }
}
