package consensus.engine;

import consensus.core.NodeId;
import consensus.cluster.progress.ReplicationState;
import consensus.protocol.role.RoleType;
import consensus.cluster.membership.MembershipConfig;

import java.util.Map;
import java.util.Optional;

/**
 * A point-in-time snapshot of a Raft node's state, for diagnostics
 * and monitoring.
 *
 * @param id                this node's id
 * @param term              the current term
 * @param votedFor          who this node voted for in the current term
 * @param role              the current protocol role
 * @param leaderId          the known leader, or empty
 * @param committedIndex    the highest committed log index
 * @param appliedIndex      the highest applied log index
 * @param leaderTransferee  the target of an in-progress leadership transfer, if any
 * @param membership        the current membership configuration
 * @param progress          per-peer replication progress (present only on the leader)
 */
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

    /**
     * Replication progress for a single peer, as seen by the leader.
     *
     * @param matchIndex the highest replicated index
     * @param nextIndex  the next index to send
     * @param sentCommit the highest commit index sent to this peer
     * @param state      the current replication state
     * @param active     whether the peer has responded recently
     */
    public record PeerStatus(
            long matchIndex,
            long nextIndex,
            long sentCommit,
            ReplicationState state,
            boolean active
    ) {}
}
