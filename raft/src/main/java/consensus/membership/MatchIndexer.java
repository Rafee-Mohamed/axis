package consensus.membership;

import consensus.node.NodeId;

import java.util.OptionalLong;

public interface MatchIndexer {
    OptionalLong match(NodeId id);
}
