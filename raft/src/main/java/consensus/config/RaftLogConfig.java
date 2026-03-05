package consensus.config;

import consensus.storage.AppliableEntriesPolicy;

public interface RaftLogConfig {
    long maxApplyingEntriesSize();
    AppliableEntriesPolicy appliableEntriesPolicy();
}
