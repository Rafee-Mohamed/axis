package io.disys.axis.consensus.sm;

import io.disys.axis.api.proto.ResponseHeader;

import java.util.function.LongSupplier;

public record NodeContext(long clusterId, long memberId, LongSupplier raftTerm) {
    public ResponseHeader header(long revision) {
        return ResponseHeader.newBuilder()
                .setClusterId(clusterId)
                .setMemberId(memberId)
                .setRevision(revision)
                .setRaftTerm(raftTerm.getAsLong())
                .build();
    }
}
