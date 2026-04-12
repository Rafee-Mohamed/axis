package io.disys.axis.consensus.transport.codec;

import com.google.protobuf.ByteString;
import io.disys.jaft.core.Snapshot;

// NOTE: jaft and proto both define a class named Snapshot.
// Proto Snapshot is fully qualified; jaft Snapshot is imported.
public final class SnapshotCodec {

    private SnapshotCodec() {}

    public static io.disys.axis.consensus.proto.Snapshot encode(Snapshot snapshot) {
        return io.disys.axis.consensus.proto.Snapshot.newBuilder()
                .setTerm(snapshot.term())
                .setIndex(snapshot.index())
                .setMembership(MembershipCodec.encodeMembershipConfig(snapshot.membership()))
                .setData(ByteString.copyFrom(snapshot.data()))
                .build();
    }

    public static Snapshot decode(io.disys.axis.consensus.proto.Snapshot proto) {
        return new Snapshot(
                proto.getTerm(),
                proto.getIndex(),
                MembershipCodec.decodeMembershipConfig(proto.getMembership()),
                proto.getData().toByteArray()
        );
    }
}
