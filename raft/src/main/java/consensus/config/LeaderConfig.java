package consensus.config;

import consensus.protocol.policy.LeaderLivenessPolicy;

public interface LeaderConfig extends PeerInflightConfig {
    long maxUncommittedSize();
    LeaderLivenessPolicy leaderLivenessPolicy();
    int heartbeatTimeout();
    int electionTimeout();
}
