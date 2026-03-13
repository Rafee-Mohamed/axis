package consensus.cluster.membership;

import consensus.core.NodeId;

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
    public long quorumCommit(Function<NodeId, OptionalLong> indexer) {
        return quorumAgreed(id -> indexer.apply(id).orElse(0), Long.MAX_VALUE);
    }


    /**
     * Evaluates quorum result — requires majority in both configs during joint consensus.
     * @param voteQuery per-voter boolean query (true = granted, false = rejected, empty = pending)
     * @return Quorum outcome for the joint configuration
     */
    public Quorum quorumResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        return quorumAgreed(id -> voteQuery.apply(id)
                .map(vote -> vote ? Quorum.REACHED : Quorum.NOT_REACHED)
                .orElse(Quorum.PENDING),
                Quorum.REACHED);
    }

    public <T extends Comparable<T>> T quorumAgreed(Function<NodeId, T> mapper, T defaultValue) {
        var currentResult = current.quorumAgreed(mapper, defaultValue);
        if (!isJoint()) return currentResult;
        var incomingResult = incoming.quorumAgreed(mapper, defaultValue);
        return currentResult.compareTo(incomingResult) <= 0 ? currentResult : incomingResult;
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