package consensus.algorithm;

public enum DataDropReason {
    NO_LEADER,
    NO_DATA,
    REMOVED_FROM_CLUSTER,
    LEADER_TRANSFER_IN_PROGRESS,
    EXCEEDS_UNCOMMITTED_SIZE,
    FORWARDING_DISABLED
}