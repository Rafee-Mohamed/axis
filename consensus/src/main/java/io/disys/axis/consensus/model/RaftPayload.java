package io.disys.axis.consensus.model;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.disys.axis.consensus.proto.Command;
import io.disys.jaft.node.tracker.TrackablePayload;

public record RaftPayload(Command command) implements TrackablePayload<Long> {

    public static RaftPayload decode(ByteString data) throws InvalidProtocolBufferException {
        return new RaftPayload(Command.parseFrom(data));
    }

    public ByteString data() {
        return command.toByteString();
    }

    @Override
    public long bytes() {
        return command.getSerializedSize();
    }

    @Override
    public Long id() {
        return command.getCommandId();
    }
}
