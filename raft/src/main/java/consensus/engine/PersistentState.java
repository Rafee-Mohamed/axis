package consensus.engine;

import consensus.core.NodeId;

import java.util.Optional;

/**
 * Hard state that must be persisted to stable storage before responding.
 *
 * <p>Changes to term or votedFor must survive crashes - losing them can
 * violate the single-vote-per-term invariant and cause split brain.</p>
 *
 * @param term     the node's current term
 * @param votedFor the node this peer voted for in the current term, or empty
 */
public record PersistentState(long term, Optional<NodeId> votedFor) {

    /** Creates an initial state with term 0 and no vote. */
    public PersistentState() {
        this(0, Optional.empty());
    }

    /**
     * Returns {@code true} if either field differs from the given values.
     *
     * @param term     the term to compare against
     * @param votedFor the voted-for to compare against
     * @return {@code true} if any field has changed
     */
    public boolean hasChanged(long term, Optional<NodeId> votedFor) {
        return this.term != term || !this.votedFor.equals(votedFor);
    }
}
