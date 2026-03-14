package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.*;
import java.util.function.Function;

/**
 * Joint configuration for two-phase membership changes.
 *
 * <p>During joint consensus, both the {@code current} (outgoing) and
 * {@code incoming} (new) majority configs must agree on quorum decisions.
 * If {@code incoming} is empty, the cluster is in a normal (non-joint)
 * configuration and only {@code current} is used.</p>
 *
 * @param current  the old/outgoing voter configuration
 * @param incoming the new voter configuration (empty if not in joint consensus)
 */
public record JointConfig(
        MajorityConfig current,
        MajorityConfig incoming
) {

    /**
     * Creates a joint config from raw voter sets, wrapping each as unmodifiable.
     *
     * @param current  the outgoing voter ids
     * @param incoming the incoming voter ids (empty for non-joint)
     * @return a new joint config
     */
    public static JointConfig of(Set<NodeId> current, Set<NodeId> incoming) {
        return new JointConfig(
                new MajorityConfig(Collections.unmodifiableSet(current)),
                new MajorityConfig(Collections.unmodifiableSet(incoming))
        );
    }

    /**
     * Returns {@code true} if this is a joint consensus configuration.
     *
     * @return {@code true} if the incoming voter set is non-empty
     */
    public boolean isJoint() {
        return !incoming.voters().isEmpty();
    }

    /**
     * Returns the highest commit index agreed by a majority in both configs.
     *
     * <p>Takes the minimum of both quorum results - the value that both
     * quorums have reached.</p>
     *
     * @param indexer maps a voter id to its match index
     * @return the quorum-committed index
     */
    public long quorumCommit(Function<NodeId, OptionalLong> indexer) {
        return quorumAgreed(id -> indexer.apply(id).orElse(0), Long.MAX_VALUE);
    }

    /**
     * Evaluates vote outcome - requires a majority in both configs during
     * joint consensus.
     *
     * @param voteQuery maps a voter id to its vote (empty if not yet received)
     * @return the quorum outcome for the joint configuration
     */
    public Quorum quorumResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        return quorumAgreed(id -> voteQuery.apply(id)
                        .map(vote -> vote ? Quorum.REACHED : Quorum.NOT_REACHED)
                        .orElse(Quorum.PENDING),
                Quorum.REACHED);
    }

    /**
     * Generic quorum agreement across both configs.
     *
     * <p>Computes quorum agreement in the current config. If in joint
     * consensus, also computes it in the incoming config and returns the
     * minimum (more conservative) of the two.</p>
     *
     * @param <T>          the value type (must be comparable)
     * @param mapper       per-voter value function
     * @param defaultValue returned for empty voter sets (vacuous agreement)
     * @return the joint quorum-agreed value
     */
    public <T extends Comparable<T>> T quorumAgreed(Function<NodeId, T> mapper, T defaultValue) {
        var currentResult = current.quorumAgreed(mapper, defaultValue);
        if (!isJoint()) return currentResult;
        var incomingResult = incoming.quorumAgreed(mapper, defaultValue);
        return currentResult.compareTo(incomingResult) <= 0 ? currentResult : incomingResult;
    }

    /**
     * Returns the union of all voter ids from both configs.
     *
     * @return all voters (current + incoming if joint)
     */
    public Set<NodeId> allVoters() {
        var allVoters = new HashSet<>(current.voters());

        if (isJoint())
            allVoters.addAll(incoming.voters());

        return allVoters;
    }

    /**
     * Returns {@code true} if the node is a voter in this joint configuration.
     *
     * <p>In normal (non-joint) mode, only the {@code current} config is
     * checked. During joint consensus, a node is a voter if it appears in
     * either {@code current} or {@code incoming}.</p>
     *
     * @param id the node to check
     * @return {@code true} if the node is a voter in either config
     */
    public boolean contains(NodeId id) {
        return current.contains(id) || (isJoint() && incoming.contains(id));
    }
}
