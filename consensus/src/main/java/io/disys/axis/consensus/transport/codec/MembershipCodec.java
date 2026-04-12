package io.disys.axis.consensus.transport.codec;

import io.disys.axis.consensus.proto.JointConfig;
import io.disys.jaft.cluster.membership.MembershipChange;
import io.disys.jaft.cluster.membership.MembershipChangeType;
import io.disys.jaft.cluster.membership.MembershipChanges;
import io.disys.jaft.cluster.membership.MembershipConfig;
import io.disys.jaft.cluster.membership.MembershipTransition;
import io.disys.jaft.core.NodeId;

import java.util.Set;
import java.util.stream.Collectors;

// NOTE: jaft and proto share identical class names for all membership types.
// Java has no import aliases, so proto membership types are fully qualified throughout.
public final class MembershipCodec {

    private MembershipCodec() {}

    // ===================== MembershipChangeType ==============================

    static io.disys.axis.consensus.proto.MembershipChangeType encodeChangeType(MembershipChangeType type) {
        return switch (type) {
            case REMOVE      -> io.disys.axis.consensus.proto.MembershipChangeType.MCT_REMOVE;
            case ADD_VOTER   -> io.disys.axis.consensus.proto.MembershipChangeType.MCT_ADD_VOTER;
            case ADD_LEARNER -> io.disys.axis.consensus.proto.MembershipChangeType.MCT_ADD_LEARNER;
        };
    }

    static MembershipChangeType decodeChangeType(io.disys.axis.consensus.proto.MembershipChangeType proto) {
        return switch (proto) {
            case MCT_REMOVE      -> MembershipChangeType.REMOVE;
            case MCT_ADD_VOTER   -> MembershipChangeType.ADD_VOTER;
            case MCT_ADD_LEARNER -> MembershipChangeType.ADD_LEARNER;
            default -> throw new IllegalArgumentException("Unrecognized MembershipChangeType: " + proto);
        };
    }

    // ===================== MembershipTransition ==============================

    static io.disys.axis.consensus.proto.MembershipTransition encodeTransition(MembershipTransition transition) {
        return switch (transition) {
            case AUTO           -> io.disys.axis.consensus.proto.MembershipTransition.MT_AUTO;
            case JOINT_AUTO     -> io.disys.axis.consensus.proto.MembershipTransition.MT_JOINT_AUTO;
            case JOINT_EXPLICIT -> io.disys.axis.consensus.proto.MembershipTransition.MT_JOINT_EXPLICIT;
            case NONE           -> io.disys.axis.consensus.proto.MembershipTransition.MT_NONE;
        };
    }

    static MembershipTransition decodeTransition(io.disys.axis.consensus.proto.MembershipTransition proto) {
        return switch (proto) {
            case MT_AUTO           -> MembershipTransition.AUTO;
            case MT_JOINT_AUTO     -> MembershipTransition.JOINT_AUTO;
            case MT_JOINT_EXPLICIT -> MembershipTransition.JOINT_EXPLICIT;
            case MT_NONE           -> MembershipTransition.NONE;
            default -> throw new IllegalArgumentException("Unrecognized MembershipTransition: " + proto);
        };
    }

    // ===================== MembershipChange ==================================

    static io.disys.axis.consensus.proto.MembershipChange encodeMembershipChange(MembershipChange change) {
        return io.disys.axis.consensus.proto.MembershipChange.newBuilder()
                .setNodeId(change.id().id())
                .setType(encodeChangeType(change.type()))
                .build();
    }

    static MembershipChange decodeMembershipChange(io.disys.axis.consensus.proto.MembershipChange proto) {
        return new MembershipChange(
                new NodeId(proto.getNodeId()),
                decodeChangeType(proto.getType())
        );
    }

    // ===================== MembershipChanges =================================

    public static io.disys.axis.consensus.proto.MembershipChanges encodeMembershipChanges(MembershipChanges changes) {
        return io.disys.axis.consensus.proto.MembershipChanges.newBuilder()
                .addAllChanges(changes.changes().stream().map(MembershipCodec::encodeMembershipChange).toList())
                .setTransition(encodeTransition(changes.transition()))
                .build();
    }

    public static MembershipChanges decodeMembershipChanges(io.disys.axis.consensus.proto.MembershipChanges proto) {
        return new MembershipChanges(
                proto.getChangesList().stream().map(MembershipCodec::decodeMembershipChange).toList(),
                decodeTransition(proto.getTransition())
        );
    }

    // ===================== MembershipConfig ==================================

    public static io.disys.axis.consensus.proto.MembershipConfig encodeMembershipConfig(MembershipConfig config) {
        var jointConfig = JointConfig.newBuilder()
                .addAllCurrentVoters(config.currentVoters().stream().map(NodeId::id).toList())
                .addAllIncomingVoters(config.incomingVoters().stream().map(NodeId::id).toList())
                .build();

        return io.disys.axis.consensus.proto.MembershipConfig.newBuilder()
                .setVoters(jointConfig)
                .addAllLearnerIds(config.learners().stream().map(NodeId::id).toList())
                .addAllNextLearnerIds(config.nextLearners().stream().map(NodeId::id).toList())
                .setTransition(encodeTransition(config.transition()))
                .build();
    }

    public static MembershipConfig decodeMembershipConfig(io.disys.axis.consensus.proto.MembershipConfig proto) {
        Set<NodeId> currentVoters  = proto.getVoters().getCurrentVotersList().stream()
                .map(NodeId::new).collect(Collectors.toUnmodifiableSet());
        Set<NodeId> incomingVoters = proto.getVoters().getIncomingVotersList().stream()
                .map(NodeId::new).collect(Collectors.toUnmodifiableSet());
        Set<NodeId> learners       = proto.getLearnerIdsList().stream()
                .map(NodeId::new).collect(Collectors.toUnmodifiableSet());
        Set<NodeId> nextLearners   = proto.getNextLearnerIdsList().stream()
                .map(NodeId::new).collect(Collectors.toUnmodifiableSet());

        return MembershipConfig.of(
                currentVoters, incomingVoters, learners, nextLearners,
                decodeTransition(proto.getTransition())
        );
    }
}
