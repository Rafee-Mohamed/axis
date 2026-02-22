package consensus.membership;

import consensus.node.NodeId;

import java.util.Set;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A set of voters that forms the majority quorum
 * @param voters - set of voters
 */
public record MajorityConfig(Set<NodeId> voters) {

    /**
     * Calculate the committed index based on match indices.
     * Returns the highest index that has been replicated to a majority.
     *
     * @param indexer indexer that retrieves matchIndex of nodeId (highest replicated index per node)
     * @return highest committed index on majority of nodes (or 0 if no majority)
     */
    public long committedIndex(MatchIndexer indexer) {
        if (voters.isEmpty()) {
            return Long.MAX_VALUE;
        }

        long[] committedIndices = voters
                .stream()
                .map(nodeId -> indexer.match(nodeId).orElse(0))
                .sorted()
                .mapToLong(Long::longValue)
                .toArray();

        var requiredMajority = committedIndices.length - majoritySize();

        return committedIndices[requiredMajority];
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
     * @param votes - Map of node votes -> result
     * @return VoteResult
     */
    public VoteResult voteResult(Map<NodeId, Boolean> votes) {
        var majorityVotes = votes.entrySet()
                .stream()
                .filter(entry -> voters.contains(entry.getKey()))
                .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.counting()));

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
