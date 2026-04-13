package io.disys.axis.lease.store;

import io.disys.axis.backend.Database;
import io.disys.axis.backend.ReadHandle;
import io.disys.axis.backend.WriteHandle;
import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.lease.codec.LeaseDataEncoder;
import io.disys.axis.lease.model.LeaseRecord;

import java.util.function.BiConsumer;

public class LeaseBackendHandle {
    private final Database db;
    private final LeaseDataEncoder encoder;
    private final LeaseDataDecoder decoder;

    public LeaseBackendHandle(Database db, LeaseDataEncoder encoder, LeaseDataDecoder decoder) {
        this.db = db;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public void put(WriteHandle wh, Lease lease) {
        wh.put(db, encoder.encodeKey(lease.id()), encoder.encodeRecord(lease.ttl(), lease.remainingTtl()));
    }

    public void consumeLeases(ReadHandle rh, BiConsumer<Long, LeaseRecord> consumer) {
        try (var it = rh.range(db)) {
            for (var kv: it) {
                var id = decoder.decodeKey(kv.key());
                var record = decoder.decodeRecord(kv.val());
                consumer.accept(id, record);
            }
        }
    }
}
