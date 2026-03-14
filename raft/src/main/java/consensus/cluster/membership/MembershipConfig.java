package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.*;
import java.util.function.Function;

/**
 * Immutable representation of the cluster's membership configuration.
 *
 * <p>A membership config tracks four groups of nodes:</p>
 * <ul>
 *   <li><b>currentVoters</b> (outgoing) - voters in the stable config, or the
 *       old voter set during joint consensus.</li>
 *   <li><b>incomingVoters</b> - the new voter set during joint consensus. Empty
 *       when not in joint consensus.</li>
 *   <li><b>learners</b> - non-voting members that receive log replication but
 *       do not participate in elections or commit decisions.</li>
 *   <li><b>nextLearners</b> - voters being demoted to learners. They remain
 *       voters until joint consensus is left, then move to {@code learners}.
 *       By invariant, nextLearners is a subset of currentVoters.</li>
 * </ul>
 *
 * <p>All invariants are validated at construction time by the {@link #of}
 * factory methods. The learner and nextLearner sets are wrapped as
 * unmodifiable, so once constructed, a config cannot be altered.</p>
 *
 * @param voters       joint voter config (current + incoming majority configs)
 * @param learners     non-voting members
 * @param nextLearners voters pending demotion to learner (subset of currentVoters)
 * @param transition   transition type ({@code NONE} for stable configs,
 *                     {@code JOINT_AUTO}/{@code JOINT_EXPLICIT} during joint consensus)
 */
public record MembershipConfig(
        JointConfig voters,
        Set<NodeId> learners,
        Set<NodeId> nextLearners,
        MembershipTransition transition
) {

    /**
     * Primary factory: validates invariants and wraps mutable sets as
     * unmodifiable before constructing the config.
     *
     * @param currentVoters  outgoing voter set
     * @param incomingVoters incoming voter set (empty if non-joint)
     * @param learners       non-voting members
     * @param nextLearners   voters pending demotion (must be subset of currentVoters)
     * @param transition     transition type
     * @return a validated, immutable membership config
     * @throws IllegalStateException if any invariant is violated
     */
    public static MembershipConfig of(
            Set<NodeId> currentVoters,
            Set<NodeId> incomingVoters,
            Set<NodeId> learners,
            Set<NodeId> nextLearners,
            MembershipTransition transition
    ) {
        validateInvariants(
          currentVoters, incomingVoters, learners, nextLearners
        );
        return new MembershipConfig(
                JointConfig.of(currentVoters, incomingVoters),
                Collections.unmodifiableSet(learners),
                Collections.unmodifiableSet(nextLearners),
                transition
        );
    }

    /**
     * Validates structural invariants that must hold for any valid config.
     *
     * <p><b>Invariant 1: nextLearners subset of currentVoters.</b>
     * A node in nextLearners is a voter being demoted. During joint consensus
     * it must still be tracked as a voter in the outgoing set (currentVoters).
     * If a nextLearner is not in currentVoters, the demotion bookkeeping is
     * broken - the node would be neither a voter nor a proper learner.</p>
     *
     * <p><b>Invariant 2: learners and voters are disjoint.</b>
     * A node cannot simultaneously be a learner and a voter. If a voter needs
     * to become a learner, it goes through nextLearners first (deferred demotion).
     * Similarly, a learner being promoted to voter must be removed from learners
     * before being added to incomingVoters.</p>
     */
    private static void validateInvariants(
            Set<NodeId> currentVoters,
            Set<NodeId> incomingVoters,
            Set<NodeId> learners,
            Set<NodeId> nextLearners
    ) {
        for (var learner: nextLearners) {
            if (!currentVoters.contains(learner)) {
                throw new IllegalStateException("Next Learner " + learner + " is not present in outgoing voters");
            }
        }

        for (var learner: learners) {
            if (currentVoters.contains(learner)) {
                throw new IllegalStateException("Learner " + learner + " is present in outgoing voters");
            }

            if (incomingVoters.contains(learner)) {
                throw new IllegalStateException("Learner " + learner + " is present in incoming voters");
            }
        }
    }

    /**
     * Convenience factory: defaults transition to {@link MembershipTransition#NONE}.
     *
     * @param currentVoters  outgoing voter set
     * @param incomingVoters incoming voter set (empty if non-joint)
     * @param learners       non-voting members
     * @param nextLearners   voters pending demotion
     * @return a validated, immutable membership config
     */
    public static MembershipConfig of(
            Set<NodeId> currentVoters,
            Set<NodeId> incomingVoters,
            Set<NodeId> learners,
            Set<NodeId> nextLearners
    ) {
        return of(currentVoters, incomingVoters, learners, nextLearners, MembershipTransition.NONE);
    }

    /**
     * Convenience factory for non-joint configs: no incoming voters or nextLearners.
     *
     * @param voters   the voter set
     * @param learners non-voting members
     * @return a validated, immutable membership config
     */
    public static MembershipConfig of(Set<NodeId> voters, Set<NodeId> learners) {
        return of(voters, Set.of(), learners, Set.of());
    }

    /**
     * Returns {@code true} if this config is in joint consensus - both the
     * current (outgoing) and incoming voter sets are active.
     *
     * <p>During joint consensus, quorum requires a majority in both sets.</p>
     *
     * @return {@code true} if in joint consensus
     */
    public boolean isJoint() {
        return voters.isJoint();
    }

    /**
     * Returns the highest commit index agreed by a majority of voters.
     *
     * <p>In non-joint mode, only the current config matters. In joint mode,
     * both configs must agree - the minimum of the two is returned.</p>
     *
     * @param indexer maps a voter id to its match index
     * @return the quorum-committed index
     * @see MajorityConfig#quorumAgreed
     */
    public long quorumCommit(Function<NodeId, OptionalLong> indexer) {
        return voters.quorumCommit(indexer);
    }

    /**
     * Generic quorum agreement, delegated to the joint voter config.
     *
     * @param <T>          the value type (must be comparable)
     * @param mapper       per-voter value function
     * @param defaultValue returned for empty voter sets
     * @return the quorum-agreed value
     */
    public <T extends Comparable<T>> T quorumAgreed(Function<NodeId, T> mapper, T defaultValue) {
        return voters.quorumAgreed(mapper, defaultValue);
    }

    /**
     * Evaluates the outcome of a vote by requiring a majority in both configs.
     *
     * <p>In non-joint mode, checks the current config only. In joint mode,
     * the vote succeeds only if it wins in both the old and new configs.</p>
     *
     * @param voteQuery maps a voter id to its vote (empty if not yet received)
     * @return the quorum outcome
     */
    public Quorum quorumResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        return voters.quorumResult(voteQuery);
    }

    /**
     * Returns all nodes in this config - voters (both configs if joint),
     * learners, and nextLearners.
     *
     * <p>This is the set of nodes the leader must track progress for and
     * replicate entries to. nextLearners are included because they are still
     * voters in the outgoing config (subset of currentVoters), but listing
     * them explicitly ensures they are not missed when building progress
     * maps.</p>
     *
     * @return the full set of member node ids
     */
    public Set<NodeId> members() {
        var members = new HashSet<>(voters.allVoters());
        members.addAll(learners);
        members.addAll(nextLearners);
        return members;
    }

    /**
     * Returns {@code true} if the node is a member of the cluster - either
     * a voter (in any config during joint consensus) or a learner.
     *
     * <p>Does not check {@code nextLearners} separately. By invariant, any
     * node in nextLearners is also in the outgoing voter set, so
     * {@code voters.contains(id)} will find it.</p>
     *
     * @param id the node to check
     * @return {@code true} if the node is a voter or learner
     */
    public boolean isMember(NodeId id) {
        return voters.contains(id) || learners.contains(id);
    }

    /**
     * Returns the membership type of the given node.
     *
     * @param id the node to check
     * @return {@link MemberType#VOTER} if the node is a voter, {@link MemberType#LEARNER} otherwise
     */
    public MemberType memberType(NodeId id) {
        return voters.contains(id) ? MemberType.VOTER : MemberType.LEARNER;
    }

    /**
     * Returns {@code true} if the node is a learner (non-voting member).
     *
     * @param id the node to check
     * @return {@code true} if the node is in the learner set
     */
    public boolean isLearner(NodeId id) {
        return learners.contains(id);
    }

    /**
     * Returns {@code true} if the node is a voter in either the current
     * (outgoing) or incoming config.
     *
     * @param id the node to check
     * @return {@code true} if the node is a voter
     */
    public boolean isVoter(NodeId id) {
        return voters.contains(id);
    }

    /**
     * Returns the incoming voter set if in joint consensus, empty set otherwise.
     *
     * @return the incoming voter ids
     */
    public Set<NodeId> incomingVoters() {
        return isJoint() ? voters.incoming().voters() : Set.of();
    }

    /**
     * Returns the current (outgoing) voter set.
     *
     * @return the current voter ids
     */
    public Set<NodeId> currentVoters() {
        return voters.current().voters();
    }

    /**
     * Returns {@code true} if this is a single-node cluster.
     *
     * @return {@code true} if total voter count is 1
     */
    public boolean isSingleton() {
        return voters.allVoters().size() == 1;
    }
}
