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
    public long committedIndex(MatchIndexer indexer) {
        var currentCommittedIndex = current.committedIndex(indexer);

        if (!isJoint())
            return currentCommittedIndex;

        var incomingCommittedIndex = incoming.committedIndex(indexer);
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

    /**
     * Checks if a node is a voter in this joint configuration.
     *
     * <p>In normal (non-joint) mode, only the {@code current} config exists
     * — the node must be in {@code current}. During joint consensus, a node
     * is a voter if it appears in either {@code current} (the outgoing/old
     * config) or {@code incoming} (the new config being adopted). Both
     * configs participate in elections and commit decisions until the joint
     * state is left.</p>
     *
     * @param id the node to check
     * @return true if the node is a voter in current, or (if joint) in incoming
     */
    public boolean contains(NodeId id) {
        return current.contains(id) || (isJoint() && incoming.contains(id));
    }
}