package consensus.engine;

import consensus.algorithm.NodeId;

import java.util.Optional;

public record PersistentState(long term, Optional<NodeId> votedFor) {
    public PersistentState() {
        this(0, Optional.empty());
    }
}
