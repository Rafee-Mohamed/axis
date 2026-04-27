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
import io.disys.axis.mvcc.model.CountOptions;
import io.disys.axis.mvcc.model.Page;
import io.disys.axis.mvcc.model.RangeOptions;
import io.disys.axis.mvcc.store.Reader;
import io.disys.axis.mvcc.model.SnapshotResult;
import io.disys.axis.mvcc.model.SortDirection;
import io.disys.axis.mvcc.model.SortTarget;

public final class StoreReader {

    private StoreReader() {}

    static GetResponse get(Reader reader, GetRequest req) {
        var result = reader.get(req.getKey().toByteArray());
        var response = GetResponse.newBuilder();
        result.ifPresent(r -> response.setKv(KeyValCodec.toKeyVal(r)));
        return response.build();
    }

    static GetAtResponse getAt(Reader reader, GetAtRequest req) {
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

    static RangeResponse range(Reader reader, RangeRequest req) {
        var page = req.hasOptions()
                ? reader.range(req.getFrom().toByteArray(), req.getTo().toByteArray(), toRangeOptions(req.getOptions()))
                : reader.range(req.getFrom().toByteArray(), req.getTo().toByteArray());
        return RangeResponse.newBuilder()
                .addAllKvs(page.items().stream().map(KeyValCodec::toKeyVal).toList())
                .setMore(page.more())
                .build();
    }

    static RangeAtResponse rangeAt(Reader reader, RangeAtRequest req) {
        var result = req.hasOptions()
                ? reader.rangeAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toRangeOptions(req.getOptions()))
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

    static KeysResponse keys(Reader reader, KeysRequest req) {
        var page = req.hasOptions()
                ? reader.keys(req.getFrom().toByteArray(), req.getTo().toByteArray(), toRangeOptions(req.getOptions()))
                : reader.keys(req.getFrom().toByteArray(), req.getTo().toByteArray());
        return KeysResponse.newBuilder()
                .addAllKeys(page.items().stream().map(ByteString::copyFrom).toList())
                .setMore(page.more())
                .build();
    }

    static KeysAtResponse keysAt(Reader reader, KeysAtRequest req) {
        var result = req.hasOptions()
                ? reader.keysAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toRangeOptions(req.getOptions()))
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

    static CountResponse count(Reader reader, CountRequest req) {
        var count = req.hasOptions()
                ? reader.count(req.getFrom().toByteArray(), req.getTo().toByteArray(), toCountOptions(req.getOptions()))
                : reader.count(req.getFrom().toByteArray(), req.getTo().toByteArray());
        return CountResponse.newBuilder().setCount(count).build();
    }

    static CountAtResponse countAt(Reader reader, CountAtRequest req) {
        var result = req.hasOptions()
                ? reader.countAt(req.getFrom().toByteArray(), req.getTo().toByteArray(), req.getRevision(), toCountOptions(req.getOptions()))
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

    static RangeOptions toRangeOptions(io.disys.axis.api.proto.RangeOptions opts) {
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
            builder.sortTarget(toSortTarget(opts.getSort().getTarget()));
            builder.sortDirection(toSortDirection(opts.getSort().getDirection()));
        }
        return builder.build();
    }

    static CountOptions toCountOptions(io.disys.axis.api.proto.CountOptions opts) {
        var builder = CountOptions.builder();
        if (opts.hasModifiedIn()) {
            builder.modifiedIn(opts.getModifiedIn().getMin(), opts.getModifiedIn().getMax());
        }
        if (opts.hasCreatedIn()) {
            builder.createdIn(opts.getCreatedIn().getMin(), opts.getCreatedIn().getMax());
        }
        return builder.build();
    }

    private static SortTarget toSortTarget(io.disys.axis.api.proto.SortTarget target) {
        return switch (target) {
            case KEY -> SortTarget.KEY;
            case VERSION -> SortTarget.VERSION;
            case CREATED_REVISION -> SortTarget.CREATED_REVISION;
            case MODIFIED_REVISION -> SortTarget.MODIFIED_REVISION;
            case VAL -> SortTarget.VAL;
            default -> SortTarget.KEY;
        };
    }

    private static SortDirection toSortDirection(io.disys.axis.api.proto.SortDirection direction) {
        return switch (direction) {
            case ASCENDING -> SortDirection.ASCENDING;
            case DESCENDING -> SortDirection.DESCENDING;
            default -> SortDirection.ASCENDING;
        };
    }
}
