package consensus.membership;

import consensus.algorithm.NodeId;

import java.util.*;


/**
 * Pure configuration transformer for membership changes.
 *
 * <p>Computes a new {@link MembershipConfig} from an existing config and a set
 * of changes. Does not touch peer progress — that reconciliation is handled
 * separately by the leader after the new config is produced.</p>
 *
 * <p>Supports two protocols:</p>
 * <ul>
 *   <li><b>Simple</b> — single-phase change. At most one voter may differ
 *       between the old and new configs (the symmetric difference constraint).
 *       The result is a non-joint config.</li>
 *   <li><b>Joint consensus</b> — two-phase change. First, {@link #executeProtocol}
 *       produces a joint config where both the old (current) and new (incoming)
 *       voter sets coexist. Later, {@link #leaveJoint()} drops the old set and
 *       promotes the new set as the sole config.</li>
 * </ul>
 *
 * <p>Protocol selection is driven by the {@link MembershipTransition} on the
 * proposed changes:</p>
 * <ul>
 *   <li>{@code AUTO} — the changer picks simple if the voter symmetric
 *       difference is ≤ 1, otherwise joint.</li>
 *   <li>{@code JOINT_AUTO} / {@code JOINT_EXPLICIT} — always joint.</li>
 * </ul>
 *
 * <p>This class operates on mutable copies of the config sets, so the original
 * {@link MembershipConfig} is never modified. The resulting config is validated
 * at construction time by {@link MembershipConfig#of}.</p>
 */
public class MembershipChanger {
    private final Set<NodeId> incomingVoters;
    private final Set<NodeId> currentVoters;
    private final Set<NodeId> learners;
    private final Set<NodeId> nextLearners;

    public MembershipChanger(MembershipConfig membership) {
        currentVoters = new HashSet<>(membership.currentVoters());
        incomingVoters = new HashSet<>(membership.incomingVoters());
        learners = new HashSet<>(membership.learners());
        nextLearners = new HashSet<>(membership.nextLearners());
    }

    /**
     * Applies the given changes and selects the appropriate protocol.
     *
     * <p>Must only be called when the config is <b>not</b> in joint consensus
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
     *   <li>For joint: keep both sets — the result is a joint config.</li>
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
        // Seed the incoming set with the current voters. Changes are applied
        // to incoming only — current is preserved as the "old" config for
        // joint consensus, or overwritten by incoming for simple changes.
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
     * incomingVoters is empty (non-joint). Transition is set to NONE.</p>
     */
    private MembershipConfig simple() {
        transferMembers();
        return buildMembership(MembershipTransition.NONE);
    }

    /**
     * Joint consensus entry: keep both current and incoming voter sets.
     *
     * <p>The result is a joint config where quorum requires majority in
     * <b>both</b> sets. The transition type (AUTO/EXPLICIT) determines
     * whether the leave-joint phase is triggered automatically or manually.</p>
     */
    private MembershipConfig enterJoint(MembershipTransition mt) {
        return buildMembership(mt);
    }

    /**
     * Counts the symmetric difference between current and incoming voter sets.
     *
     * <p>This is |currentVoters △ incomingVoters| — the number of voters
     * that are in one set but not the other. For the simple protocol, this
     * must be ≤ 1 (at most one voter added or removed).</p>
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

    private MembershipConfig buildMembership(MembershipTransition mt) {
        return MembershipConfig.of(currentVoters, incomingVoters, learners, nextLearners, mt);

    }

    /**
     * Leaves joint consensus: drops the old config, promotes the new one.
     *
     * <p>Must only be called when the config <b>is</b> in joint consensus
     * (i.e. {@code incomingVoters} is non-empty). This is the second phase
     * of a two-phase membership change.</p>
     *
     * <p>{@link #transferMembers()} promotes incomingVoters → currentVoters
     * and moves nextLearners → learners. Nodes that were in the old config
     * (currentVoters) but not in the new config (incomingVoters) and not in
     * learners are effectively removed — they won't appear in the result,
     * and the leader will drop their progress during reconciliation.</p>
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
     * Promotes incoming → current and nextLearners → learners.
     *
     * <p>Used by both {@link #simple()} (where it finalizes the single-phase
     * change) and {@link #leaveJoint()} (where it finalizes the second phase
     * of joint consensus).</p>
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
     * If the node was a learner or nextLearner, it is removed from those sets.
     */
    private void addVoter(NodeId id) {
        incomingVoters.add(id);
        learners.remove(id);
        nextLearners.remove(id);
    }

    /**
     * Demotes or adds a node as a learner.
     *
     * <p>If the node is currently a voter in the <b>outgoing</b> config
     * (currentVoters), it cannot be immediately made a learner — that would
     * mean tracking it as both voter and learner simultaneously. Instead, it
     * is placed in {@code nextLearners} and will become a learner when
     * {@link #leaveJoint()} is called.</p>
     *
     * <p>If the node is not in the outgoing config, it is added directly
     * to {@code learners}.</p>
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
     * <p>The node is <b>not</b> removed from currentVoters — during joint
     * consensus, nodes in the outgoing config must remain until
     * {@link #leaveJoint()} is called. Nodes only in currentVoters (and not
     * in the incoming config or learners) will be dropped when the joint
     * state is left.</p>
     */
    private void remove(NodeId id) {
        incomingVoters.remove(id);
        learners.remove(id);
        nextLearners.remove(id);
    }



}
