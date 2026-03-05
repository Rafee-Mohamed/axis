package consensus.membership;

import consensus.algorithm.NodeId;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A set of voters that forms the majority quorum
 * @param voters - set of voters
 */
public record MajorityConfig(Set<NodeId> voters) {

    /**
     * Calculate the committed index based on match indices.
     * Returns the highest value that a majority of voters have reached.
     *
     * <p>General-purpose majority agreement: given a per-voter value (via the
     * indexer), sorts them and picks the value at the majority position. This
     * is the highest value where at least {@link #majoritySize()} voters have
     * that value or higher.</p>
     *
     * <p>Used for:</p>
     * <ul>
     *   <li>Log commit: indexer returns each voter's match index</li>
     *   <li>Read index confirmation: indexer returns each voter's acked heartbeat seq</li>
     * </ul>
     *
     * @param indexer retrieves the per-voter value (empty means the voter is absent/unknown)
     * @return highest value agreed upon by a majority (or 0 if no majority)
     */
    public long majorityAgreed(Function<NodeId, OptionalLong> indexer) {
        if (voters.isEmpty()) {
            return Long.MAX_VALUE;
        }

        long[] values = voters
                .stream()
                .map(nodeId -> indexer.apply(nodeId).orElse(0))
                .sorted()
                .mapToLong(Long::longValue)
                .toArray();

        var requiredMajority = values.length - majoritySize();

        return values[requiredMajority];
    }

    /** @return Number of votes needed for majority */
    public int majoritySize() {
        return voters.size() / 2 + 1;
    }

    /**
     * Check if we have enough votes for majority.
     * @param votesReceived - nodes that voted
     * @return has enough votes
     */
    public boolean hasMajority(Set<NodeId> votesReceived) {
        long count = voters.stream()
                .filter(votesReceived::contains)
                .count();
        return count >= majoritySize();
    }


    /**
     * Calculates the vote result given a set of votes results for each node
     * @param voteQuery - Map of node votes -> result
     * @return VoteResult
     */
    public VoteResult voteResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        var majorityVotes = voters
                .stream()
                .filter(id -> voteQuery.apply(id).isPresent())
                .map(id -> voteQuery.apply(id).orElse(false))
                .collect(Collectors.groupingBy(vote -> vote, Collectors.counting()));

        var majority = majoritySize();

        if (majorityVotes.getOrDefault(true, 0L) >= majority)
            return VoteResult.WON;

        if (majorityVotes.getOrDefault(false, 0L) >= majority)
            return VoteResult.LOST;

        return VoteResult.PENDING;
    }

    public boolean contains(NodeId id) {
        return voters.contains(id);
    }
}
