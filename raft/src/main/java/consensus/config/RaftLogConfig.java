package consensus.config;

import consensus.storage.AppliableEntriesPolicy;

/**
 * Configuration surface required by the Raft log for determining
 * when and how many committed entries can be surfaced for application.
 */
public interface RaftLogConfig {

    /**
     * Maximum total byte size of entries that can be in the process
     * of being applied at once. Limits memory pressure from large
     * committed batches.
     *
     * @return the applying entries size limit in bytes
     */
    long maxApplyingEntriesSize();

    /**
     * Policy controlling which committed entries are eligible for
     * application - whether entries must only be committed, or must
     * also be persisted locally before being surfaced.
     *
     * @return the appliable entries policy
     * @see AppliableEntriesPolicy
     */
    AppliableEntriesPolicy appliableEntriesPolicy();
}
