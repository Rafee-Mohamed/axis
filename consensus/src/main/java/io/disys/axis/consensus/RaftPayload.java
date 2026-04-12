package io.disys.axis.consensus;

import io.disys.jaft.storage.Payload;

public record RaftPayload(byte[] data) implements Payload {

    @Override
    public long bytes() {
        return data.length;
    }
}
