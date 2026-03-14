package consensus.config;

/**
 * Configuration for per-peer inflight entry tracking. Controls how
 * many entries (and how many bytes) the leader can have in-flight
 * to a single peer before pausing replication to that peer.
 */
public interface PeerInflightConfig {

    /**
     * Maximum number of inflight append messages per peer.
     *
     * @return the inflight message count limit
     */
    int maxInflightMsgs();

    /**
     * Maximum total byte size of inflight entries per peer. Acts as
     * a secondary cap alongside {@link #maxInflightMsgs()} to prevent
     * a few large entries from consuming excessive memory.
     *
     * @return the inflight byte size limit
     */
    long maxInflightBytes();
}
