package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

/**
 * A set of voters that forms a majority quorum.
 *
 * <p>
 * Provides generic quorum agreement over any {@link Comparable} value,
 * used for both commit index calculation (numeric) and vote tallying
 * ({@link Quorum} enum).
 * </p>
 *
 * @param voters the set of voter node ids
 */
public record MajorityConfig(Set<NodeId> voters) {

    /**
     * Generic quorum agreement: maps each voter to a {@link Comparable}
     * value, sorts them, and returns the value at the quorum position -
     * the highest value where at least {@link #quorumSize()} voters are
     * at or above it.
     *
     * @param <T>          the value type (must be comparable)
     * @param mapper       per-voter value function
     * @param defaultValue returned when the voter set is empty (vacuous agreement)
     * @return the quorum-agreed value
     */
    public <T extends Comparable<T>> T quorumAgreed(Function<NodeId, T> mapper, T defaultValue) {
        return voters
                .stream()
                .map(mapper)
                .sorted()
                .skip(voters.size() - quorumSize())
                .findFirst()
                .orElse(defaultValue);
    }

    /**
     * Returns the number of votes needed for a majority.
     *
     * @return the quorum size ({@code voters.size() / 2 + 1})
     */
    public int quorumSize() {
        return voters.size() / 2 + 1;
    }

    /**
     * Returns the highest commit index that a quorum of voters have reached.
     *
     * <p>
     * Maps each voter to its match index (0 if unknown) and returns
     * the quorum-agreed value. Empty voter set returns
     * {@code Long.MAX_VALUE} (vacuously committed).
     * </p>
     *
     * @param matchIndexer maps a voter id to its match index
     * @return the quorum-committed index
     */
    public long quorumCommit(Function<NodeId, OptionalLong> matchIndexer) {
        return quorumAgreed(id -> matchIndexer.apply(id).orElse(0), Long.MAX_VALUE);
    }

    /**
     * Evaluates the result of a vote across this voter set.
     *
     * <p>
     * Maps each voter's boolean vote to a {@link Quorum} value
     * ({@code true} to {@link Quorum#REACHED}, {@code false} to
     * {@link Quorum#NOT_REACHED}, absent to {@link Quorum#PENDING})
     * and returns the quorum-agreed outcome.
     * </p>
     *
     * @param quorumQuery maps a voter id to its vote (empty if not yet received)
     * @return the quorum outcome
     */
    public Quorum quorumResult(Function<NodeId, Optional<Boolean>> quorumQuery) {
        return quorumAgreed(id -> quorumQuery.apply(id)
                .map(result -> result ? Quorum.REACHED : Quorum.NOT_REACHED)
                .orElse(Quorum.PENDING), Quorum.REACHED);
    }

    /**
     * Returns {@code true} if the given node is a voter in this config.
     *
     * @param id the node to check
     * @return {@code true} if present
     */
    public boolean contains(NodeId id) {
        return voters.contains(id);
    }
}
