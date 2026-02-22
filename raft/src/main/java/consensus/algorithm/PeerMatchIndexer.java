package consensus.algorithm;

import consensus.membership.MatchIndexer;
import consensus.node.NodeId;

import java.util.Map;
import java.util.OptionalLong;

public record PeerMatchIndexer(Map<NodeId, PeerProgress> peerProgress) implements MatchIndexer {
    @Override
    public OptionalLong match(NodeId id) {
        var progress = peerProgress.get(id);
        return progress == null ? OptionalLong.empty() : OptionalLong.of(progress.match());
    }
}
