package consensus.membership;

import consensus.node.NodeId;

import java.util.*;
import java.util.function.Function;

/**
 * Immutable representation of the cluster's membership configuration.
 *
 * <p>A membership config tracks four groups of nodes:</p>
 * <ul>
 *   <li><b>currentVoters</b> (outgoing) — voters in the stable config, or the
 *       old voter set during joint consensus.</li>
 *   <li><b>incomingVoters</b> — the new voter set during joint consensus. Empty
 *       when not in joint consensus.</li>
 *   <li><b>learners</b> — non-voting members that receive log replication but
 *       do not participate in elections or commit decisions.</li>
 *   <li><b>nextLearners</b> — voters being demoted to learners. They remain
 *       voters until joint consensus is left, then move to {@code learners}.
 *       By invariant, nextLearners ⊆ currentVoters.</li>
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
 *                     {@code AUTO}/{@code EXPLICIT} during joint consensus)
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
     * @param nextLearners   voters pending demotion (must be ⊆ currentVoters)
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
     * <p>Invariant 1: nextLearners ⊆ currentVoters. A node in nextLearners
     * is a voter being <em>demoted</em>. During joint consensus it must still
     * be tracked as a voter in the outgoing set (currentVoters). If a
     * nextLearner is not in currentVoters, the demotion bookkeeping is broken
     * — the node would be neither a voter nor a proper learner.</p>
     *
     * <p>Invariant 2: learners ∩ (currentVoters ∪ incomingVoters) = ∅. A
     * node cannot simultaneously be a learner and a voter. If a voter needs to
     * become a learner, it goes through nextLearners first (deferred demotion).
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

    /** Convenience factory: defaults transition to {@link MembershipTransition#NONE}. */
    public static MembershipConfig of(
            Set<NodeId> currentVoters,
            Set<NodeId> incomingVoters,
            Set<NodeId> learners,
            Set<NodeId> nextLearners
    ) {
        return of(currentVoters, incomingVoters, learners, nextLearners, MembershipTransition.NONE);
    }

    /** Convenience factory for non-joint configs: no incoming voters or nextLearners. */
    public static MembershipConfig of(Set<NodeId> voters, Set<NodeId> learners) {
        return of(voters, Set.of(), learners, Set.of());
    }


    /**
     * Returns {@code true} if this config is in joint consensus — i.e. both
     * the current (outgoing) and incoming voter sets are active. During joint
     * consensus, quorum requires majority in <b>both</b> sets.
     */
    public boolean isJoint() {
        return voters.isJoint();
    }

    /**
     * Returns the highest value that a majority of voters have reached.
     *
     * <p>In non-joint mode, only the current config matters. In joint mode,
     * both configs must agree — the minimum of the two is returned.</p>
     *
     * @see MajorityConfig#majorityAgreed for the algorithm
     */
    public long majorityAgreed(Function<NodeId, OptionalLong> indexer) {
        return voters.majorityAgreed(indexer);
    }

    /**
     * Evaluates the outcome of a vote by requiring majority in both configs.
     *
     * <p>In non-joint mode, checks the current config only. In joint mode,
     * the vote succeeds only if it wins in both the old and new configs.</p>
     */
    public VoteResult voteResult(Function<NodeId, Optional<Boolean>> voteQuery) {
        return voters.voteResult(voteQuery);
    }

    /**
     * Returns all nodes in this config — voters (both configs if joint),
     * learners, and nextLearners.
     *
     * <p>This is the set of nodes the leader must track progress for and
     * replicate entries to. Note: nextLearners are included because they
     * are still voters in the outgoing config (subset of currentVoters),
     * but listing them explicitly ensures they are not missed when building
     * progress maps.</p>
     */
    public Set<NodeId> members() {
        var members = new HashSet<>(voters.allVoters());
        members.addAll(learners);
        members.addAll(nextLearners);
        return members;
    }

    /**
     * Checks if a node is a member of the cluster — either a voter (in any
     * config during joint consensus) or a learner.
     *
     * <p>Does NOT check {@code nextLearners}. By invariant, any node in
     * nextLearners is guaranteed to also be in the outgoing voter set
     * (the {@code current} config during joint consensus), so
     * {@code voters.contains(id)} will find it.</p>
     *
     * @param id the node to check
     * @return true if the node is a voter or learner in this configuration
     */
    public boolean isMember(NodeId id) {
        return voters.contains(id) || learners.contains(id);
    }

    /**
     * Returns {@code true} if the node is a learner (non-voting member).
     */
    public boolean isLearner(NodeId id) {
        return learners.contains(id);
    }

    /**
     * Returns {@code true} if the node is a voter in either the current
     * (outgoing) or incoming config.
     */
    public boolean isVoter(NodeId id) { return voters.contains(id); }

    /**
     * Returns the incoming voter set if in joint consensus, empty set otherwise.
     */
    public Set<NodeId> incomingVoters() {
        return isJoint() ? voters.incoming().voters() : Set.of();
    }

    /**
     * Returns the current (outgoing) voter set.
     */
    public Set<NodeId> currentVoters() {
        return voters.current().voters();
    }

    public boolean isSingleton() {
        return voters.allVoters().size() == 1;
    }
}
