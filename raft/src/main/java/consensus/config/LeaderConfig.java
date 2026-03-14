package consensus.config;

import consensus.protocol.policy.LeaderLivenessPolicy;

/**
 * Configuration surface required by
 * {@link consensus.protocol.role.Leader}. Extends
 * {@link PeerInflightConfig} since the leader manages per-peer
 * inflight entry tracking.
 */
public interface LeaderConfig extends PeerInflightConfig {

    /**
     * Maximum total byte size of uncommitted entries. When exceeded,
     * new proposals are rejected to prevent unbounded memory growth.
     *
     * @return the uncommitted size limit in bytes
     */
    long maxUncommittedSize();

    /**
     * Policy controlling whether the leader actively verifies it
     * still holds a quorum of responsive peers.
     *
     * @return the leader liveness policy
     * @see LeaderLivenessPolicy
     */
    LeaderLivenessPolicy leaderLivenessPolicy();

    /**
     * Heartbeat interval in ticks. The leader broadcasts heartbeats
     * to all peers at this cadence.
     *
     * @return the heartbeat timeout
     */
    int heartbeatTimeout();

    /**
     * Election timeout in ticks. Used by the leader for the
     * quorum-check timer and leadership transfer timeout window.
     *
     * @return the election timeout
     */
    int electionTimeout();
}
