package io.disys.axis.lease.store;

import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.VersionedStore;

import java.time.Clock;
import java.util.Map;
import java.util.function.Consumer;

public class LeaseStoreState {
    final Clock clock;
    final Map<Long, Lease> leases;
    final LeaseBackendHandle bh;
    final LeaseStoreConfig config;

    public LeaseStoreState(Clock clock, Map<Long, Lease> leases, LeaseBackendHandle bh, LeaseStoreConfig config) {
        this.clock = clock;
        this.leases = leases;
        this.bh = bh;
        this.config = config;
    }

    public Consumer<Record> storeRecordConsumer() {
        var decoder = new LeaseDataDecoder();
        return record -> {
            if (record.tombstone()) return;
            var id = decoder.decodeVal(record.val());
            leases.computeIfPresent(id, (_, lease) -> {
                lease.addItem(record.key());
                return lease;
            });
        };
    }

    public LeaseStore toStore(VersionedStore store) {
        return LeaseStore.from(this, store);
    }
}
