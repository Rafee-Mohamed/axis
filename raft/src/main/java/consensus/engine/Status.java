package consensus.engine;

import consensus.core.NodeId;
import consensus.cluster.progress.ReplicationState;
import consensus.protocol.role.RoleType;
import consensus.cluster.membership.MembershipConfig;

import java.util.Map;
import java.util.Optional;

public record Status(
        NodeId id,
        long term,
        Optional<NodeId> votedFor,
        RoleType role,
        Optional<NodeId> leaderId,
        long committedIndex,
        long appliedIndex,
        Optional<NodeId> leaderTransferee,
        MembershipConfig membership,
        Optional<Map<NodeId, PeerStatus>> progress
) {
    public record PeerStatus(
            long matchIndex,
            long nextIndex,
            long sentCommit,
            ReplicationState state,
            boolean active
    ) {}
}
