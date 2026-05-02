package io.disys.axis.consensus.sm;

import io.disys.axis.api.proto.*;
import io.disys.axis.consensus.proto.Command;
import io.disys.axis.consensus.proto.LeaseCheckpoint;
import io.disys.axis.lease.model.*;
import io.disys.axis.lease.store.LeaseStore;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.VersionedStore;
import io.disys.axis.mvcc.store.Writer;

import java.util.Arrays;
import java.util.Optional;

public final class StoreWriter {
    private final NodeContext context;
    private final LeaseStore leaseStore;
    private final VersionedStore store;
    private final StoreReader storeReader;

    public StoreWriter(NodeContext context, LeaseStore leaseStore, VersionedStore store) {
        this.context = context;
        this.leaseStore = leaseStore;
        this.store = store;
        this.storeReader = new StoreReader(context);
    }

    public Object apply(Writer writer, Command cmd) {
        return switch (cmd.getTypeCase()) {
            case PUT -> put(writer, cmd.getPut());
            case PUT_AND_GET -> putAndGet(writer, cmd.getPutAndGet());
            case PUT_WITH_LEASE -> putWithLease(writer, cmd.getPutWithLease());
            case PUT_WITH_LEASE_AND_GET -> putWithLeaseAndGet(writer, cmd.getPutWithLeaseAndGet());
            case UPDATE_LEASE -> updateLease(writer, cmd.getUpdateLease());
            case UPDATE_VALUE -> updateValue(writer, cmd.getUpdateValue());
            case REMOVE_LEASE -> removeLease(writer, cmd.getRemoveLease());
            case DELETE -> delete(writer, cmd.getDelete());
            case DELETE_AND_GET -> deleteAndGet(writer, cmd.getDeleteAndGet());
            case DELETE_RANGE -> deleteRange(writer, cmd.getDeleteRange());
            case DELETE_RANGE_AND_GET -> deleteRangeAndGet(writer, cmd.getDeleteRangeAndGet());
            case TXN -> txn(writer, cmd.getTxn());
            case COMPACT -> compact(cmd.getCompact(), writer.revision());
            case LEASE_GRANT -> leaseGrant(writer, cmd.getLeaseGrant());
            case LEASE_REVOKE -> leaseRevoke(writer, cmd.getLeaseRevoke());
            case LEASE_CHECKPOINT -> { leaseCheckpoint(cmd.getLeaseCheckpoint()); yield null; }
            default -> throw new IllegalStateException("Unknown command type: " + cmd.getTypeCase());
        };
    }

    private PutResponse put(Writer writer, PutRequest req) {
        writer.put(req.getKey().toByteArray(), KeyValCodec.encode(req.getVal()).toByteArray());
        return PutResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .build();
    }

    private PutAndGetResponse putAndGet(Writer writer, PutAndGetRequest req) {
        var prev = writer.putAndGet(req.getKey().toByteArray(), KeyValCodec.encode(req.getVal()).toByteArray());
        var response = PutAndGetResponse.newBuilder()
                .setHeader(context.header(writer.revision()));
        prev.ifPresent(r -> response.setPrevKv(KeyValCodec.toKeyVal(r)));
        return response.build();
    }

    private PutResponse putWithLease(Writer writer, PutWithLeaseRequest req) {
        var key = req.getKey().toByteArray();
        writer.put(key, KeyValCodec.encode(req.getVal(), req.getLeaseId()).toByteArray());
        leaseStore.attach(req.getLeaseId(), key);
        return PutResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .build();
    }

    private PutAndGetResponse putWithLeaseAndGet(Writer writer, PutWithLeaseAndGetRequest req) {
        var key = req.getKey().toByteArray();
        var prev = writer.putAndGet(key, KeyValCodec.encode(req.getVal(), req.getLeaseId()).toByteArray());
        leaseStore.attach(req.getLeaseId(), key);
        var response = PutAndGetResponse.newBuilder()
                .setHeader(context.header(writer.revision()));
        prev.ifPresent(r -> response.setPrevKv(KeyValCodec.toKeyVal(r)));
        return response.build();
    }

    private PutResponse updateLease(Writer writer, UpdateLeaseRequest req) {
        var key = req.getKey().toByteArray();
        var current = writer.get(key);
        if (current.isPresent()) {
            var raw = current.get().val();
            var oldLeaseId = KeyValCodec.decodeLeaseId(raw);
            if (oldLeaseId != 0) {
                leaseStore.detach(oldLeaseId, key);
            }
            writer.put(key, KeyValCodec.encode(KeyValCodec.decodeVal(raw), req.getLeaseId()).toByteArray());
            leaseStore.attach(req.getLeaseId(), key);
        }
        return PutResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .build();
    }

    private PutResponse updateValue(Writer writer, UpdateValueRequest req) {
        var key = req.getKey().toByteArray();
        var current = writer.get(key);
        if (current.isPresent()) {
            var currentLeaseId = KeyValCodec.decodeLeaseId(current.get().val());
            writer.put(key, KeyValCodec.encode(req.getVal(), currentLeaseId).toByteArray());
        }
        return PutResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .build();
    }

    private PutResponse removeLease(Writer writer, RemoveLeaseRequest req) {
        var key = req.getKey().toByteArray();
        var current = writer.get(key);
        if (current.isPresent()) {
            var raw = current.get().val();
            var currentLeaseId = KeyValCodec.decodeLeaseId(raw);
            if (currentLeaseId != 0) {
                leaseStore.detach(currentLeaseId, key);
                writer.put(key, KeyValCodec.encode(KeyValCodec.decodeVal(raw), 0L).toByteArray());
            }
        }
        return PutResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .build();
    }

    private DeleteResponse delete(Writer writer, DeleteRequest req) {
        var deleted = writer.delete(req.getKey().toByteArray());
        return DeleteResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .setDeleted(deleted)
                .build();
    }

    private DeleteAndGetResponse deleteAndGet(Writer writer, DeleteAndGetRequest req) {
        var prev = writer.deleteAndGet(req.getKey().toByteArray());
        var response = DeleteAndGetResponse.newBuilder()
                .setHeader(context.header(writer.revision()));
        prev.ifPresent(r -> response.setPrevKv(KeyValCodec.toKeyVal(r)));
        return response.build();
    }

    private DeleteRangeResponse deleteRange(Writer writer, DeleteRangeRequest req) {
        var deleted = writer.deleteRange(req.getFrom().toByteArray(), req.getTo().toByteArray());
        return DeleteRangeResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .setDeleted(deleted)
                .build();
    }

    private DeleteRangeAndGetResponse deleteRangeAndGet(Writer writer, DeleteRangeAndGetRequest req) {
        var prevs = writer.deleteRangeAndGet(req.getFrom().toByteArray(), req.getTo().toByteArray());
        return DeleteRangeAndGetResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .addAllPrevKvs(prevs.stream().map(KeyValCodec::toKeyVal).toList())
                .build();
    }

    private TxnResponse txn(Writer writer, TxnRequest req) {
        var succeeded = req.getConditionsList().stream().allMatch(c -> evaluate(writer, c));
        var ops = succeeded ? req.getSuccessList() : req.getFailureList();
        var results = ops.stream().map(op -> applyTxnOp(writer, op)).toList();
        return TxnResponse.newBuilder()
                .setHeader(context.header(writer.revision()))
                .setSucceeded(succeeded)
                .addAllResults(results)
                .build();
    }

    private TxnOpResult applyTxnOp(Writer writer, TxnOp op) {
        return switch (op.getOpCase()) {
            case GET -> TxnOpResult.newBuilder().setGet(storeReader.get(writer, op.getGet())).build();
            case GET_AT -> TxnOpResult.newBuilder().setGetAt(storeReader.getAt(writer, op.getGetAt())).build();
            case RANGE -> TxnOpResult.newBuilder().setRange(storeReader.range(writer, op.getRange())).build();
            case RANGE_AT -> TxnOpResult.newBuilder().setRangeAt(storeReader.rangeAt(writer, op.getRangeAt())).build();
            case KEYS -> TxnOpResult.newBuilder().setKeys(storeReader.keys(writer, op.getKeys())).build();
            case KEYS_AT -> TxnOpResult.newBuilder().setKeysAt(storeReader.keysAt(writer, op.getKeysAt())).build();
            case COUNT -> TxnOpResult.newBuilder().setCount(storeReader.count(writer, op.getCount())).build();
            case COUNT_AT -> TxnOpResult.newBuilder().setCountAt(storeReader.countAt(writer, op.getCountAt())).build();
            case PUT -> TxnOpResult.newBuilder().setPut(put(writer, op.getPut())).build();
            case PUT_WITH_LEASE -> TxnOpResult.newBuilder().setPut(putWithLease(writer, op.getPutWithLease())).build();
            case UPDATE_LEASE -> TxnOpResult.newBuilder().setPut(updateLease(writer, op.getUpdateLease())).build();
            case UPDATE_VALUE -> TxnOpResult.newBuilder().setPut(updateValue(writer, op.getUpdateValue())).build();
            case REMOVE_LEASE -> TxnOpResult.newBuilder().setPut(removeLease(writer, op.getRemoveLease())).build();
            case DELETE -> TxnOpResult.newBuilder().setDelete(delete(writer, op.getDelete())).build();
            case DELETE_RANGE -> TxnOpResult.newBuilder().setDeleteRange(deleteRange(writer, op.getDeleteRange())).build();
            case TXN -> TxnOpResult.newBuilder().setTxn(txn(writer, op.getTxn())).build();
            default -> TxnOpResult.newBuilder().build();
        };
    }

    private boolean evaluate(Writer writer, Condition cond) {
        return switch (cond.getScopeCase()) {
            case KEY -> satisfies(writer.get(cond.getKey().toByteArray()), cond);
            case RANGE -> {
                var page = writer.range(cond.getRange().getFrom().toByteArray(), cond.getRange().getTo().toByteArray());
                yield page.items().isEmpty() || page.items().stream().allMatch(r -> satisfies(Optional.of(r), cond));
            }
            default -> true;
        };
    }

    private boolean satisfies(Optional<Record> record, Condition cond) {
        return switch (cond.getTargetCase()) {
            case VERSION -> compare(
                    record.map(r -> (long) r.version()).orElse(0L), cond.getVersion(), cond.getOp());
            case CREATED_REVISION -> compare(
                    record.map(Record::createdAtSeq).orElse(0L), cond.getCreatedRevision(), cond.getOp());
            case MODIFIED_REVISION -> compare(
                    record.map(Record::modifiedAtSeq).orElse(0L), cond.getModifiedRevision(), cond.getOp());
            case VAL -> compareBytes(
                    record.map(r -> KeyValCodec.decodeVal(r.val()).toByteArray()).orElse(new byte[0]),
                    cond.getVal().toByteArray(), cond.getOp());
            case LEASE_ID -> compare(
                    record.map(r -> KeyValCodec.decodeLeaseId(r.val())).orElse(0L), cond.getLeaseId(), cond.getOp());
            default -> true;
        };
    }

    private static boolean compare(long actual, long expected, CompareOp op) {
        return switch (op) {
            case EQ -> actual == expected;
            case NE -> actual != expected;
            case GT -> actual > expected;
            case LT -> actual < expected;
            case GTE -> actual >= expected;
            case LTE -> actual <= expected;
            default -> false;
        };
    }

    private static boolean compareBytes(byte[] actual, byte[] expected, CompareOp op) {
        int cmp = Arrays.compareUnsigned(actual, expected);
        return switch (op) {
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
            case GT -> cmp > 0;
            case LT -> cmp < 0;
            case GTE -> cmp >= 0;
            case LTE -> cmp <= 0;
            default -> false;
        };
    }

    private CompactResponse compact(CompactRequest req, long revision) {
        store.compact(req.getRevision());
        return CompactResponse.newBuilder()
                .setHeader(context.header(revision))
                .build();
    }

    private GrantResponse leaseGrant(Writer writer, GrantRequest req) {
        var result = leaseStore.grant(writer.handle(), req.getLeaseId(), req.getTtl());
        return switch (result) {
            case GrantResult.LeaseGranted g -> GrantResponse.newBuilder()
                    .setHeader(context.header(writer.revision()))
                    .setLeaseId(g.id())
                    .setTtl(req.getTtl())
                    .build();
            case GrantResult.LeaseAlreadyExists e ->
                throw new IllegalStateException("Lease already exists: " + e.id());
        };
    }

    private RevokeResponse leaseRevoke(Writer writer, RevokeRequest req) {
        var id = req.getLeaseId();
        leaseStore.leasedKeys(id).forEach(key -> writer.delete(key));
        var result = leaseStore.revoke(writer.handle(), id);
        return switch (result) {
            case RevokeResult.LeaseRevoked _ -> RevokeResponse.newBuilder()
                    .setHeader(context.header(writer.revision()))
                    .setRevoked(LeaseRevoked.newBuilder().setLeaseId(id))
                    .build();
            case RevokeResult.LeaseNotFound _ -> RevokeResponse.newBuilder()
                    .setHeader(context.header(writer.revision()))
                    .setNotFound(LeaseNotFound.newBuilder().setLeaseId(id))
                    .build();
            case RevokeResult.LeaseRevokeInProgress _ ->
                throw new IllegalStateException("Unexpected state during lease revoke: " + id);
        };
    }

    private void leaseCheckpoint(LeaseCheckpoint req) {
        leaseStore.checkpoint(req.getLeaseId(), req.getRemainingTtl());
    }
}
