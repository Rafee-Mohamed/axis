package consensus.core;

import java.util.Optional;

public record RaftState(
        NodeId id,
        long term,
        long committedIndex,
        Optional<NodeId> votedFor
) {
}
