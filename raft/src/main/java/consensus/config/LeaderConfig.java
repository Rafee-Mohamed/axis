package consensus.config;

import consensus.algorithm.LeaderLivenessPolicy;

public interface LeaderConfig extends PeerInflightConfig {
    long maxUncommittedSize();
    LeaderLivenessPolicy leaderLivenessPolicy();
    int heartbeatTimeout();
    int electionTimeout();
}
