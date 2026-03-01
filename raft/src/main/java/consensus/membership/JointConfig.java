package consensus.membership;

import consensus.node.NodeId;

import java.util.*;
import java.util.function.Function;

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

    public static JointConfig of(Set<NodeId> current, Set<NodeId> incoming) {
        return new JointConfig(
                new MajorityConfig(Collections.unmodifiableSet(current)),
                new MajorityConfig(Collections.unmodifiableSet(incoming))
        );
    }

    /**
     * Check if we're in joint consensus mode.
     */
    public boolean isJoint() {
        return !incoming.voters().isEmpty();
    }

    /**
     * Requires majority agreement in BOTH configs during joint consensus.
     * Takes the minimum of both — the value that both quorums have reached.
     */
    public long majorityAgreed(Function<NodeId, OptionalLong> indexer) {
        var currentAgreed = current.majorityAgreed(indexer);

        if (!isJoint())
            return currentAgreed;

        var incomingAgreed = incoming.majorityAgreed(indexer);
        return Math.min(currentAgreed, incomingAgreed);
    }


    /**
     * Calculates Vote result - requires majority in both configs during joint consensus.
     * @param voteQuery - Map of node votes -> result
     * @return VoteResult
     */
    public VoteResult voteResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        var currentVoteResult = current.voteResult(voteQuery);

        if (!isJoint())
            return currentVoteResult;

        var incomingResult = incoming.voteResult(voteQuery);

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