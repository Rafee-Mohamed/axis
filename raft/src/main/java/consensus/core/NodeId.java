package consensus.core;

/**
 * Unique identifier for a node in the Raft cluster.
 *
 * <h4>Never reuse an id</h4>
 * <p>A node id is permanent - once used, it must never be assigned to
 * another node, even after the original node has been removed. A
 * removed node may still be alive (partitioned, unaware of its
 * removal). If a new node takes the same id, the old machine can
 * still reach other nodes using its stale address book and send
 * messages with {@code from = NodeId(5)}. Receivers identify the
 * sender by the {@code from} field, not the source address, so they
 * accept these as coming from the current node 5. The result: two
 * physical machines both cast votes as "node 5" in a future term,
 * breaking the single-vote invariant and potentially electing two
 * leaders.</p>
 *
 * <h4>Constraints</h4>
 * <p>The id must be non-zero - zero is reserved as a sentinel
 * meaning "no node." The application is responsible for assigning
 * globally unique, never-reused ids to each node.</p>
 *
 * @param id the numeric node identifier, must be non-zero
 */
public record NodeId(long id) {}
