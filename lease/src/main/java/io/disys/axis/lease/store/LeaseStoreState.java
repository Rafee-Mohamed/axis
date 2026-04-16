package io.disys.axis.lease.store;

import io.disys.axis.lease.codec.LeaseDataDecoder;
import io.disys.axis.mvcc.model.Record;
import io.disys.axis.mvcc.store.VersionedStore;

import java.time.Clock;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;

public class LeaseStoreState {
    final Clock clock;
    final Map<Long, Lease> leases;
    final LeaseBackendHandle bh;
    final LeaseStoreConfig config;
    final RandomGenerator rand;

    public LeaseStoreState(Clock clock, Map<Long, Lease> leases, LeaseBackendHandle bh, LeaseStoreConfig config, RandomGenerator rand) {
        this.clock = clock;
        this.leases = leases;
        this.bh = bh;
        this.config = config;
        this.rand = rand;
    }

    public Consumer<Record> storeRecordConsumer() {
        var decoder = new LeaseDataDecoder();
        return record -> {
            if (record.tombstone()) return;
            var id = decoder.decodeVal(record.val());
            leases.computeIfPresent(id, (_, lease) -> {
                lease.attach(record.key());
                return lease;
            });
        };
    }
}
