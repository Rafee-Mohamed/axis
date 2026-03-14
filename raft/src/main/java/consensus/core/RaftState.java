package consensus.core;

import java.util.Optional;

/**
 * Persisted hard state of a Raft node. Must be durably written to
 * stable storage before any messages from the corresponding term are
 * sent or acknowledged.
 *
 * <p>After a restart, the node recovers its identity, current term,
 * vote, and commit progress from this state.</p>
 *
 * <p>{@code term} and {@code votedFor} are <b>safety-critical</b> -
 * they must be persisted to prevent double-voting and term regression.
 * {@code committedIndex} is a <b>performance optimization</b> - it
 * is not required for safety (the leader will re-inform the node of
 * the commit index via heartbeats after restart), but persisting it
 * allows the node to resume applying committed entries to the state
 * machine immediately without waiting for leader contact.</p>
 *
 * @param id             this node's identifier
 * @param term           the current term (monotonically increasing,
 *                       safety-critical)
 * @param committedIndex the highest log index known to be committed
 *                       (persisted for fast recovery, not safety)
 * @param votedFor       the node this node voted for in the current
 *                       term, or empty if no vote was cast
 *                       (safety-critical)
 */
public record RaftState(
        NodeId id,
        long term,
        long committedIndex,
        Optional<NodeId> votedFor
) {
}
