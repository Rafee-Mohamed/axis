package io.disys.axis.consensus.log;

import com.google.protobuf.InvalidProtocolBufferException;
import io.disys.axis.consensus.proto.WalRecord;
import io.disys.axis.consensus.transport.codec.EntryCodec;
import io.disys.axis.consensus.transport.codec.MembershipCodec;
import io.disys.jaft.core.NodeId;
import io.disys.jaft.core.Snapshot;
import io.disys.jaft.engine.CheckpointState;
import io.disys.jaft.engine.PersistentState;
import io.disys.jaft.storage.Entry;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Encodes and decodes {@link WalRecord} proto messages to and from {@link ByteBuffer}.
 *
 * <p>Each encode method produces a single {@code ByteBuffer} containing the
 * serialized proto bytes for one WAL record. The decode method reverses this,
 * returning the proto message for the caller to switch on.</p>
 *
 * <p>The codec is stateless - all methods are static.</p>
 */
public final class WalRecordCodec {

    private WalRecordCodec() {}

    // ===================== Encode =============================================

    public static ByteBuffer encodeHardState(PersistentState state) {
        var proto = WalRecord.newBuilder()
                .setHardState(WalRecord.HardState.newBuilder()
                        .setTerm(state.term())
                        .setVotedFor(state.votedFor().map(NodeId::id).orElse(0L))
                        .build())
                .build();
        return ByteBuffer.wrap(proto.toByteArray());
    }

    public static ByteBuffer encodeCheckpoint(CheckpointState state) {
        var proto = WalRecord.newBuilder()
                .setCheckpoint(WalRecord.Checkpoint.newBuilder()
                        .setCommittedIndex(state.commit())
                        .build())
                .build();
        return ByteBuffer.wrap(proto.toByteArray());
    }

    public static ByteBuffer encodeSnapshot(Snapshot snapshot) {
        var proto = WalRecord.newBuilder()
                .setSnapshot(WalRecord.SnapshotMeta.newBuilder()
                        .setTerm(snapshot.term())
                        .setIndex(snapshot.index())
                        .setMembership(MembershipCodec.encodeMembershipConfig(snapshot.membership()))
                        .build())
                .build();
        return ByteBuffer.wrap(proto.toByteArray());
    }

    public static ByteBuffer encodeEntry(Entry entry) {
        var proto = WalRecord.newBuilder()
                .setEntry(EntryCodec.encode(entry))
                .build();
        return ByteBuffer.wrap(proto.toByteArray());
    }

    // ===================== Decode =============================================

    /**
     * Decodes a {@link ByteBuffer} into a {@link WalRecord}.
     *
     * @param buf the buffer positioned at the start of a serialized record
     * @return the decoded proto record; switch on {@code getTypeCase()} to handle each variant
     * @throws WalDecodeException if the bytes cannot be parsed as a valid {@link WalRecord}
     */
    public static WalRecord decode(ByteBuffer buf) {
        try {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return WalRecord.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new WalDecodeException(e);
        }
    }

    // ===================== Decode to jaft types ===============================

    public static PersistentState decodePersistentState(WalRecord.HardState hs) {
        var votedFor = hs.getVotedFor() == 0
                ? Optional.<NodeId>empty()
                : Optional.of(new NodeId(hs.getVotedFor()));
        return new PersistentState(hs.getTerm(), votedFor);
    }

    public static Snapshot decodeSnapshot(WalRecord.SnapshotMeta sm) {
        return new Snapshot(
                sm.getTerm(),
                sm.getIndex(),
                MembershipCodec.decodeMembershipConfig(sm.getMembership()),
                new byte[0]
        );
    }
}
