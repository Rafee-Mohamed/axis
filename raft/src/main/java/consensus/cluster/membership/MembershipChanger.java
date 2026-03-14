package consensus.cluster.membership;

import consensus.core.NodeId;

import java.util.*;


/**
 * Pure configuration transformer for membership changes.
 *
 * <p>Computes a new {@link MembershipConfig} from an existing config and a set
 * of changes. Does not touch peer progress - that reconciliation is handled
 * separately by the leader after the new config is produced.</p>
 *
 * <p>Supports two protocols:</p>
 * <ul>
 *   <li><b>Simple</b> - single-phase change. At most one voter may differ
 *       between the old and new configs (the symmetric difference constraint).
 *       The result is a non-joint config.</li>
 *   <li><b>Joint consensus</b> - two-phase change. First,
 *       {@link #executeProtocol} produces a joint config where both old
 *       (current) and new (incoming) voter sets coexist. Later,
 *       {@link #leaveJoint()} drops the old set and promotes the new set
 *       as the sole config.</li>
 * </ul>
 *
 * <p>Protocol selection is driven by the {@link MembershipTransition} on the
 * proposed changes:</p>
 * <ul>
 *   <li>{@link MembershipTransition#AUTO} - the changer picks simple if the
 *       voter symmetric difference is at most 1, otherwise joint.</li>
 *   <li>{@link MembershipTransition#JOINT_AUTO} /
 *       {@link MembershipTransition#JOINT_EXPLICIT} - always joint.</li>
 * </ul>
 *
 * <p>This class operates on mutable copies of the config sets, so the original
 * {@link MembershipConfig} is never modified.</p>
 */
public class MembershipChanger {

    /** Mutable copy of the incoming (new) voter set. */
    private final Set<NodeId> incomingVoters;

    /** Mutable copy of the current (outgoing) voter set. */
    private final Set<NodeId> currentVoters;

    /** Mutable copy of the learner set. */
    private final Set<NodeId> learners;

    /** Voters pending demotion to learner after leaving joint consensus. */
    private final Set<NodeId> nextLearners;

    /**
     * Creates a changer with mutable copies of the given config's sets.
     *
     * @param membership the current membership configuration to change from
     */
    public MembershipChanger(MembershipConfig membership) {
        currentVoters = new HashSet<>(membership.currentVoters());
        incomingVoters = new HashSet<>(membership.incomingVoters());
        learners = new HashSet<>(membership.learners());
        nextLearners = new HashSet<>(membership.nextLearners());
    }

    /**
     * Applies the given changes and selects the appropriate protocol.
     *
     * <p>Must only be called when the config is not in joint consensus
     * (i.e. {@code incomingVoters} is empty). If the config is already joint,
     * the only valid operation is {@link #leaveJoint()}.</p>
     *
     * <p>Workflow:</p>
     * <ol>
     *   <li>Copy currentVoters into incomingVoters (both sets now identical).</li>
     *   <li>Apply each change (add voter, add learner, remove) to incomingVoters
     *       and learner sets.</li>
     *   <li>Select protocol based on transition and symmetric difference.</li>
     *   <li>For simple: promote incomingVoters to currentVoters, clear incoming.</li>
     *   <li>For joint: keep both sets - the result is a joint config.</li>
     * </ol>
     *
     * @param mc the proposed changes and desired transition
     * @return the new membership config (validated at construction)
     * @throws IllegalStateException if already in joint consensus, or if all
     *         voters would be removed
     */
    public MembershipConfig executeProtocol(MembershipChanges mc) {
        if (!incomingVoters.isEmpty()) {
            throw new IllegalStateException("Membership is in joint consensus, can't start membership protocol while in execution of a protocol - Joint consensus");
        }
        incomingVoters.addAll(currentVoters);
        applyChanges(mc);

        if (mc.transition() == MembershipTransition.AUTO && votersSymmetricDifference() <= 1) {
            return simple();
        }
        return enterJoint(mc.transition());

    }

    /**
     * Simple protocol: promote the incoming config as the sole config.
     *
     * <p>After this, currentVoters = what was incomingVoters, and
     * incomingVoters is empty (non-joint). Transition is set to
     * {@link MembershipTransition#NONE}.</p>
     *
     * @return the new non-joint membership config
     */
    private MembershipConfig simple() {
        transferMembers();
        return buildMembership(MembershipTransition.NONE);
    }

    /**
     * Joint consensus entry: keep both current and incoming voter sets.
     *
     * <p>The result is a joint config where quorum requires a majority in
     * both sets.</p>
     *
     * @param mt the transition type ({@link MembershipTransition#JOINT_AUTO}
     *           or {@link MembershipTransition#JOINT_EXPLICIT})
     * @return the new joint membership config
     */
    private MembershipConfig enterJoint(MembershipTransition mt) {
        return buildMembership(mt);
    }

    /**
     * Counts the symmetric difference between current and incoming voter sets.
     *
     * <p>This is the number of voters that are in one set but not the other.
     * For the simple protocol, this must be at most 1.</p>
     *
     * @return the symmetric difference size
     */
    public int votersSymmetricDifference() {
        var diff = new HashSet<>(currentVoters);
        diff.removeAll(incomingVoters);

        for (var voter: incomingVoters) {
            if (!currentVoters.contains(voter)) {
                diff.add(voter);
            }
        }

        return diff.size();

    }

    /**
     * Builds a {@link MembershipConfig} from the current mutable state.
     *
     * @param mt the transition type for the resulting config
     * @return a validated membership config
     */
    private MembershipConfig buildMembership(MembershipTransition mt) {
        return MembershipConfig.of(currentVoters, incomingVoters, learners, nextLearners, mt);

    }

    /**
     * Leaves joint consensus: drops the old config, promotes the new one.
     *
     * <p>Must only be called when the config is in joint consensus (i.e.
     * {@code incomingVoters} is non-empty). This is the second phase of a
     * two-phase membership change.</p>
     *
     * <p>{@link #transferMembers()} promotes incomingVoters to currentVoters
     * and moves nextLearners to learners. Nodes that were in the old config
     * but not in the new config and not in learners are effectively removed -
     * the leader will drop their progress during reconciliation.</p>
     *
     * @return the new non-joint membership config
     * @throws IllegalStateException if not in joint consensus
     */
    public MembershipConfig leaveJoint() {
        if (incomingVoters.isEmpty()) {
            throw new IllegalStateException("Cannot leave joint in a non-joint membership");
        }

        transferMembers();
        return buildMembership(MembershipTransition.NONE);
    }

    /**
     * Applies individual changes to the incoming config sets.
     *
     * @param mc the changes to apply
     * @throws IllegalStateException if all voters would be removed
     */
    private void applyChanges(MembershipChanges mc) {
        for (var change: mc.changes()) {
            switch (change.type()) {
                case MembershipChangeType.ADD_VOTER -> addVoter(change.id());
                case MembershipChangeType.ADD_LEARNER -> addLearner(change.id());
                case MembershipChangeType.REMOVE -> remove(change.id());
            }
        }

        if (incomingVoters.isEmpty()) {
            throw new IllegalStateException("No incoming voters - removed all voters");
        }
    }

    /**
     * Promotes incoming to current and nextLearners to learners.
     *
     * <p>Used by both {@link #simple()} (finalizes single-phase change)
     * and {@link #leaveJoint()} (finalizes the second phase of joint
     * consensus).</p>
     */
    private void transferMembers() {
        currentVoters.clear();
        currentVoters.addAll(incomingVoters);
        incomingVoters.clear();

        learners.addAll(nextLearners);
        nextLearners.clear();
    }

    /**
     * Adds or promotes a node to voter in the incoming config.
     *
     * <p>If the node was a learner or nextLearner, it is removed from those sets.</p>
     *
     * @param id the node to add as voter
     */
    private void addVoter(NodeId id) {
        incomingVoters.add(id);
        learners.remove(id);
        nextLearners.remove(id);
    }

    /**
     * Demotes or adds a node as a learner.
     *
     * <p>If the node is currently a voter in the outgoing config
     * (currentVoters), it cannot be immediately made a learner - that would
     * mean tracking it as both voter and learner simultaneously. Instead it
     * is placed in {@code nextLearners} and becomes a learner when
     * {@link #leaveJoint()} is called.</p>
     *
     * <p>If the node is not in the outgoing config, it is added directly
     * to {@code learners}.</p>
     *
     * @param id the node to add as learner
     */
    private void addLearner(NodeId id) {
        incomingVoters.remove(id);
        learners.remove(id);
        nextLearners.remove(id);

        if (currentVoters.contains(id)) {
            nextLearners.add(id);
        } else {
            learners.add(id);
        }
    }

    /**
     * Removes a node from the incoming config, learners, and nextLearners.
     *
     * <p>The node is not removed from currentVoters - during joint consensus,
     * nodes in the outgoing config must remain until {@link #leaveJoint()} is
     * called. Nodes only in currentVoters will be dropped when the joint state
     * is left.</p>
     *
     * @param id the node to remove
     */
    private void remove(NodeId id) {
        incomingVoters.remove(id);
        learners.remove(id);
        nextLearners.remove(id);
    }
}
