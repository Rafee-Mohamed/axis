package consensus.protocol.rejection;

/**
 * Why a read index request was rejected by the protocol.
 */
public enum ReadDropReason {

    /** No leader is known - cannot confirm leadership for a linearizable read. */
    NO_LEADER
}
