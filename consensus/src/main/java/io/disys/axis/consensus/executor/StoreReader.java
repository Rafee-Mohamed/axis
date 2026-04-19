package io.disys.axis.consensus.executor;

import com.google.protobuf.ByteString;
import io.disys.axis.api.proto.Compacted;
import io.disys.axis.api.proto.CountAtRequest;
import io.disys.axis.api.proto.CountAtResponse;
import io.disys.axis.api.proto.CountRequest;
import io.disys.axis.api.proto.CountResponse;
import io.disys.axis.api.proto.CountResult;
import io.disys.axis.api.proto.Future;
import io.disys.axis.api.proto.GetAtOk;
import io.disys.axis.api.proto.GetAtRequest;
import io.disys.axis.api.proto.GetAtResponse;
import io.disys.axis.api.proto.GetRequest;
import io.disys.axis.api.proto.GetResponse;
import io.disys.axis.api.proto.KeysAtRequest;
import io.disys.axis.api.proto.KeysAtResponse;
import io.disys.axis.api.proto.KeysRequest;
import io.disys.axis.api.proto.KeysResponse;
import io.disys.axis.api.proto.KeysResult;
import io.disys.axis.api.proto.RangeAtRequest;
import io.disys.axis.api.proto.RangeAtResponse;
import io.disys.axis.api.proto.RangeRequest;
import io.disys.axis.api.proto.RangeResponse;
import io.disys.axis.api.proto.RangeResult;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.CountOptions;
import io.disys.axis.mvcc.store.Page;
import io.disys.axis.mvcc.store.RangeOptions;
import io.disys.axis.mvcc.store.SnapshotResult;
import io.disys.axis.mvcc.store.SortDirection;
import io.disys.axis.mvcc.store.SortTarget;
import io.disys.axis.mvcc.store.VersionedStore;

public final class StoreReader {

    private final VersionedStore store;

    public StoreReader(VersionedStore store) {
        this.store = store;
    }

    public GetResponse get(GetRequest req) {
        try (var reader = store.reader()) {
            var result = reader.get(req.getKey().toByteArray());
            var response = GetResponse.newBuilder();
            result.ifPresent(r -> response.setKv(KeyValCodec.toKeyVal(r)));
            return response.build();
        }
    }

    public GetAtResponse getAt(GetAtRequest req) {
        try (var reader = store.reader()) {
            var result = reader.getAt(req.getKey().toByteArray(), req.getRevision());
            return switch (result) {
                case SnapshotResult.Ok<java.util.Optional<Record>> ok -> {
                    var ok_ = GetAtOk.newBuilder();
                    ok.value().ifPresent(r -> ok_.setKv(KeyValCodec.toKeyVal(r)));
                    yield GetAtResponse.newBuilder().setOk(ok_).build();
                }
                case SnapshotResult.Compacted<java.util.Optional<Record>> c -> GetAtResponse.newBuilder()
                        .setCompacted(Compacted.newBuilder()
                                .setFirstVisibleRevision(c.firstVisibleCommitSeq())
                                .setRequestedRevision(c.requestedCommitSeq()))
                        .build();
                case SnapshotResult.Future<java.util.Optional<Record>> f -> GetAtResponse.newBuilder()
                        .setFuture(Future.newBuilder()
                                .setLastVisibleRevision(f.lastVisibleCommitSeq())
                                .setRequestedRevision(f.requestedCommitSeq()))
                        .build();
            };
        }
    }

    public RangeResponse range(RangeRequest req) {
        try (var reader = store.reader()) {
            var page = req.hasOptions()
                    ? reader.range(req.getFrom().toByteArray(), req.getTo().toByteArray(), toMvccRangeOptions(req.getOptions()))
                    : reader.range(req.getFrom().toByteArray(), req.getTo().toByteArray());
            return RangeResponse.newBuilder()
                    .addAllKvs(page.items().stream().map(KeyValCodec::toKeyVal).toList())
                    .setMore(page.more())
                    .build();
        }
    }

    public RangeAtResponse rangeAt(RangeAtRequest req) {
        try (var reader = store.reader()) {
            var result = req.hasOptions()
                    ? reader.rangeAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toMvccRangeOptions(req.getOptions()))
                    : reader.rangeAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision());
            return switch (result) {
                case SnapshotResult.Ok<Page<Record>> ok -> RangeAtResponse.newBuilder()
                        .setOk(RangeResult.newBuilder()
                                .addAllKvs(ok.value().items().stream().map(KeyValCodec::toKeyVal).toList())
                                .setMore(ok.value().more()))
                        .build();
                case SnapshotResult.Compacted<Page<Record>> c -> RangeAtResponse.newBuilder()
                        .setCompacted(Compacted.newBuilder()
                                .setFirstVisibleRevision(c.firstVisibleCommitSeq())
                                .setRequestedRevision(c.requestedCommitSeq()))
                        .build();
                case SnapshotResult.Future<Page<Record>> f -> RangeAtResponse.newBuilder()
                        .setFuture(Future.newBuilder()
                                .setLastVisibleRevision(f.lastVisibleCommitSeq())
                                .setRequestedRevision(f.requestedCommitSeq()))
                        .build();
            };
        }
    }

    public KeysResponse keys(KeysRequest req) {
        try (var reader = store.reader()) {
            var page = req.hasOptions()
                    ? reader.keys(req.getFrom().toByteArray(), req.getTo().toByteArray(), toMvccRangeOptions(req.getOptions()))
                    : reader.keys(req.getFrom().toByteArray(), req.getTo().toByteArray());
            return KeysResponse.newBuilder()
                    .addAllKeys(page.items().stream().map(ByteString::copyFrom).toList())
                    .setMore(page.more())
                    .build();
        }
    }

    public KeysAtResponse keysAt(KeysAtRequest req) {
        try (var reader = store.reader()) {
            var result = req.hasOptions()
                    ? reader.keysAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toMvccRangeOptions(req.getOptions()))
                    : reader.keysAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision());
            return switch (result) {
                case SnapshotResult.Ok<Page<byte[]>> ok -> KeysAtResponse.newBuilder()
                        .setOk(KeysResult.newBuilder()
                                .addAllKeys(ok.value().items().stream().map(ByteString::copyFrom).toList())
                                .setMore(ok.value().more()))
                        .build();
                case SnapshotResult.Compacted<Page<byte[]>> c -> KeysAtResponse.newBuilder()
                        .setCompacted(Compacted.newBuilder()
                                .setFirstVisibleRevision(c.firstVisibleCommitSeq())
                                .setRequestedRevision(c.requestedCommitSeq()))
                        .build();
                case SnapshotResult.Future<Page<byte[]>> f -> KeysAtResponse.newBuilder()
                        .setFuture(Future.newBuilder()
                                .setLastVisibleRevision(f.lastVisibleCommitSeq())
                                .setRequestedRevision(f.requestedCommitSeq()))
                        .build();
            };
        }
    }

    public CountResponse count(CountRequest req) {
        try (var reader = store.reader()) {
            var count = req.hasOptions()
                    ? reader.count(req.getFrom().toByteArray(), req.getTo().toByteArray(), toMvccCountOptions(req.getOptions()))
                    : reader.count(req.getFrom().toByteArray(), req.getTo().toByteArray());
            return CountResponse.newBuilder().setCount(count).build();
        }
    }

    public CountAtResponse countAt(CountAtRequest req) {
        try (var reader = store.reader()) {
            var result = req.hasOptions()
                    ? reader.countAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toMvccCountOptions(req.getOptions()))
                    : reader.countAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision());
            return switch (result) {
                case SnapshotResult.Ok<Long> ok -> CountAtResponse.newBuilder()
                        .setOk(CountResult.newBuilder().setCount(ok.value()))
                        .build();
                case SnapshotResult.Compacted<Long> c -> CountAtResponse.newBuilder()
                        .setCompacted(Compacted.newBuilder()
                                .setFirstVisibleRevision(c.firstVisibleCommitSeq())
                                .setRequestedRevision(c.requestedCommitSeq()))
                        .build();
                case SnapshotResult.Future<Long> f -> CountAtResponse.newBuilder()
                        .setFuture(Future.newBuilder()
                                .setLastVisibleRevision(f.lastVisibleCommitSeq())
                                .setRequestedRevision(f.requestedCommitSeq()))
                        .build();
            };
        }
    }

    private static RangeOptions toMvccRangeOptions(io.disys.axis.api.proto.RangeOptions opts) {
        var builder = RangeOptions.builder();
        if (opts.getLimit() > 0) {
            builder.limit(opts.getLimit());
        }
        if (opts.hasModifiedIn()) {
            builder.modifiedIn(opts.getModifiedIn().getMin(), opts.getModifiedIn().getMax());
        }
        if (opts.hasCreatedIn()) {
            builder.createdIn(opts.getCreatedIn().getMin(), opts.getCreatedIn().getMax());
        }
        if (opts.hasSort()) {
            builder.sortTarget(toMvccSortTarget(opts.getSort().getTarget()));
            builder.sortDirection(toMvccSortDirection(opts.getSort().getDirection()));
        }
        return builder.build();
    }

    private static CountOptions toMvccCountOptions(io.disys.axis.api.proto.CountOptions opts) {
        var builder = CountOptions.builder();
        if (opts.hasModifiedIn()) {
            builder.modifiedIn(opts.getModifiedIn().getMin(), opts.getModifiedIn().getMax());
        }
        if (opts.hasCreatedIn()) {
            builder.createdIn(opts.getCreatedIn().getMin(), opts.getCreatedIn().getMax());
        }
        return builder.build();
    }

    private static SortTarget toMvccSortTarget(io.disys.axis.api.proto.SortTarget target) {
        return switch (target) {
            case KEY -> SortTarget.KEY;
            case VERSION -> SortTarget.VERSION;
            case CREATED_REVISION -> SortTarget.CREATED_REVISION;
            case MODIFIED_REVISION -> SortTarget.MODIFIED_REVISION;
            case VAL -> SortTarget.VAL;
            default -> SortTarget.KEY;
        };
    }

    private static SortDirection toMvccSortDirection(io.disys.axis.api.proto.SortDirection direction) {
        return switch (direction) {
            case ASCENDING -> SortDirection.ASCENDING;
            case DESCENDING -> SortDirection.DESCENDING;
            default -> SortDirection.ASCENDING;
        };
    }
}
