package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

/**
 * A set of voters that forms the majority quorum
 * @param voters - set of voters
 */
public record MajorityConfig(Set<NodeId> voters) {

    /**
     * General-purpose quorum agreement: maps each voter to a {@link Comparable}
     * value, sorts them, and returns the value at the quorum position — the
     * highest value where at least {@link #quorumSize()} voters are at or above it.
     *
     * @param mapper       per-voter value function
     * @param defaultValue returned when voters is empty (vacuously agreed)
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

    /** @return Number of votes needed for majority */
    public int quorumSize() {
        return voters.size() / 2 + 1;
    }

    public long quorumCommit(Function<NodeId, OptionalLong> matchIndexer) {
        return quorumAgreed(id -> matchIndexer.apply(id).orElse(0), Long.MAX_VALUE);
    }

    public Quorum quorumResult(Function<NodeId, Optional<Boolean>> quorumQuery) {
        return quorumAgreed(id -> quorumQuery.apply(id)
                        .map(result -> result ? Quorum.REACHED : Quorum.NOT_REACHED)
                        .orElse(Quorum.PENDING) ,Quorum.REACHED);
    }

    public boolean contains(NodeId id) {
        return voters.contains(id);
    }
}
