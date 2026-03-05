package consensus.algorithm;

import consensus.node.NodeId;

import java.util.Optional;

public record RaftState(
        NodeId id,
        long term,
        long committedIndex,
        Optional<NodeId> votedFor
) {
}
