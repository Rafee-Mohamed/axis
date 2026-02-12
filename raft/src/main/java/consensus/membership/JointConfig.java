package consensus.membership;

import consensus.node.NodeId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Joint configuration of two phase membership changes.
 * During joint consensus, both 'current' and 'incoming' majorities must agree.
 *   - current: the old configuration (voters before change)
 *   - incoming: the new configuration (voters after change)
 *  If 'incoming' is null/empty, we're in a single (non-joint) configuration.
 *
 * @param current
 * @param incoming
 */
public record JointConfig(
        MajorityConfig current,
        MajorityConfig incoming
) {

    /**
     * Check if we're in joint consensus mode.
     */
    public boolean isJoint() {
        return incoming != null && !incoming.voters().isEmpty();
    }

    /**
     * Committed index requires majority in BOTH configs during joint consensus.
     */
    public long committedIndex(Map<NodeId, Long> matchIndices) {
        var currentCommittedIndex = current.committedIndex(matchIndices);

        if (!isJoint())
            return currentCommittedIndex;

        var incomingCommittedIndex = incoming.committedIndex(matchIndices);
        // Both must agree - take the minimum
        return Math.min(currentCommittedIndex, incomingCommittedIndex);
    }


    /**
     * Calculates Vote result - requires majority in both configs during joint consensus.
     * @param votes - Map of node votes -> result
     * @return VoteResult
     */
    public VoteResult voteResult(Map<NodeId, Boolean> votes) {
        var currentVoteResult = current.voteResult(votes);

        if (!isJoint())
            return currentVoteResult;

        var incomingResult = incoming.voteResult(votes);

        // Won if both won
        if (currentVoteResult == VoteResult.WON && incomingResult == VoteResult.WON)
            return VoteResult.WON;

        // Lost if either lost
        if (currentVoteResult == VoteResult.LOST || incomingResult == VoteResult.LOST)
            return VoteResult.LOST;

        return VoteResult.PENDING;
    }


    /**
     * All voter IDs (union of both configs).
     */
    public Set<NodeId> allVoters() {
        var allVoters = new HashSet<>(current.voters());

        if (isJoint())
            allVoters.addAll(incoming.voters());

        return allVoters;
    }
}