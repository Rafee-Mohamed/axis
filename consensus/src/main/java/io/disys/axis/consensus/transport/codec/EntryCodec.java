package io.disys.axis.consensus.transport.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import io.disys.axis.consensus.model.RaftPayload;
import io.disys.axis.consensus.log.WalDecodeException;
import io.disys.jaft.storage.Entry;

// NOTE: jaft and proto both define a class named Entry.
// Proto Entry is fully qualified throughout; jaft Entry is imported.
public final class EntryCodec {

    private EntryCodec() {}

    public static io.disys.axis.consensus.proto.Entry encode(Entry entry) {
        return switch (entry) {
            case Entry.Snapshot _ ->
                throw new IllegalStateException("Entry.Snapshot is never replicated");

            case Entry.Placeholder p -> io.disys.axis.consensus.proto.Entry.newBuilder()
                    .setPlaceholder(io.disys.axis.consensus.proto.Entry.Placeholder.newBuilder()
                            .setTerm(p.term())
                            .setIndex(p.index())
                            .build())
                    .build();

            case Entry.Data d -> {
                var payload = (RaftPayload) d.data();
                yield io.disys.axis.consensus.proto.Entry.newBuilder()
                        .setData(io.disys.axis.consensus.proto.Entry.Data.newBuilder()
                                .setTerm(d.term())
                                .setIndex(d.index())
                                .setData(payload.data())
                                .build())
                        .build();
            }

            case Entry.MembershipChange mc -> io.disys.axis.consensus.proto.Entry.newBuilder()
                    .setMembershipChange(io.disys.axis.consensus.proto.Entry.MembershipChangeEntry.newBuilder()
                            .setTerm(mc.term())
                            .setIndex(mc.index())
                            .setChanges(MembershipCodec.encodeMembershipChanges(mc.membershipChanges()))
                            .build())
                    .build();

            case Entry.LeaveJoint lj -> io.disys.axis.consensus.proto.Entry.newBuilder()
                    .setLeaveJoint(io.disys.axis.consensus.proto.Entry.LeaveJoint.newBuilder()
                            .setTerm(lj.term())
                            .setIndex(lj.index())
                            .build())
                    .build();
        };
    }

    public static Entry decode(io.disys.axis.consensus.proto.Entry proto) {
        return switch (proto.getVariantCase()) {
            case PLACEHOLDER -> new Entry.Placeholder(
                    proto.getPlaceholder().getTerm(),
                    proto.getPlaceholder().getIndex()
            );

            case DATA -> {
                try {
                    yield new Entry.Data(
                            proto.getData().getTerm(),
                            proto.getData().getIndex(),
                            RaftPayload.decode(proto.getData().getData())
                    );
                } catch (InvalidProtocolBufferException e) {
                    throw new WalDecodeException(e);
                }
            }

            case MEMBERSHIP_CHANGE -> new Entry.MembershipChange(
                    proto.getMembershipChange().getTerm(),
                    proto.getMembershipChange().getIndex(),
                    MembershipCodec.decodeMembershipChanges(proto.getMembershipChange().getChanges())
            );

            case LEAVE_JOINT -> new Entry.LeaveJoint(
                    proto.getLeaveJoint().getTerm(),
                    proto.getLeaveJoint().getIndex()
            );

            case VARIANT_NOT_SET -> throw new IllegalArgumentException("Entry has no variant set");
        };
    }
}
