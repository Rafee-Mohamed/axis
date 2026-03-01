package consensus.config;

import consensus.algorithm.ElectionProtocol;
import consensus.algorithm.LeaderLivenessPolicy;
import consensus.algorithm.ProposalHandleMode;
import consensus.algorithm.ReadIndexMode;
/**
 * Immutable configuration for the Raft algorithm and its downstream components
 * (RaftLog, Inflights, etc.). If a {@code RaftConfig} instance exists, it is
 * guaranteed to be valid — all validation is performed at construction time.
 *
 * <p>Use {@link #builder()} to create instances.</p>
 */
public record RaftConfig(
        int electionTimeout,
        int heartbeatTimeout,
        ElectionProtocol electionProtocol,
        ProposalHandleMode proposalHandleMode,
        ReadIndexMode readIndexMode,
        LeaderLivenessPolicy leaderLivenessPolicy,
        long maxMsgSize,
        long maxUncommittedSize,
        int maxInflightMsgs,
        long maxInflightBytes
) implements LeaderConfig, FollowerConfig, LearnerConfig {

    public RaftConfig {
        if (heartbeatTimeout <= 0) {
            throw new IllegalArgumentException("heartbeatTimeout must be greater than 0");
        }
        if (electionTimeout <= heartbeatTimeout) {
            throw new IllegalArgumentException(
                    "electionTimeout must be greater than heartbeatTimeout");
        }
        if (maxInflightMsgs <= 0) {
            throw new IllegalArgumentException("maxInflightMsgs must be greater than 0");
        }
        if (maxInflightBytes < maxMsgSize) {
            throw new IllegalArgumentException("maxInflightBytes must be >= maxMsgSize");
        }
        if (readIndexMode == ReadIndexMode.LEASE && leaderLivenessPolicy != LeaderLivenessPolicy.QUORUM_VERIFIED) {
            throw new IllegalArgumentException(
                    "leaderLivenessPolicy must be QUORUM_VERIFIED when readIndexMode is LEASE");
        }
    }

    @Override
    public int leaseTimeout() {
        return electionTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int electionTimeout;
        private int heartbeatTimeout;
        private ElectionProtocol electionProtocol;
        private ProposalHandleMode proposalHandleMode;
        private ReadIndexMode readIndexMode;
        private LeaderLivenessPolicy leaderLivenessPolicy;
        private long maxMsgSize;
        private long maxUncommittedSize;
        private int maxInflightMsgs;
        private long maxInflightBytes;

        private Builder() {
            this.electionProtocol = ElectionProtocol.DUAL_ELECTION;
            this.proposalHandleMode = ProposalHandleMode.FORWARD_TO_LEADER;
            this.readIndexMode = ReadIndexMode.HEARTBEAT_IMMEDIATE;
            this.leaderLivenessPolicy = LeaderLivenessPolicy.UNMONITORED;
            this.maxMsgSize = Long.MAX_VALUE;
            this.maxUncommittedSize = Long.MAX_VALUE;
            this.maxInflightBytes = Long.MAX_VALUE;
        }

        public Builder electionTimeout(int electionTimeout) {
            this.electionTimeout = electionTimeout;
            return this;
        }

        public Builder heartbeatTimeout(int heartbeatTimeout) {
            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        public Builder electionProtocol(ElectionProtocol electionProtocol) {
            this.electionProtocol = electionProtocol;
            return this;
        }

        public Builder proposalHandleMode(ProposalHandleMode proposalHandleMode) {
            this.proposalHandleMode = proposalHandleMode;
            return this;
        }

        public Builder readIndexMode(ReadIndexMode readIndexMode) {
            this.readIndexMode = readIndexMode;
            return this;
        }

        public Builder leaderLivenessPolicy(LeaderLivenessPolicy leaderLivenessPolicy) {
            this.leaderLivenessPolicy = leaderLivenessPolicy;
            return this;
        }

        public Builder maxMsgSize(long maxMsgSize) {
            this.maxMsgSize = maxMsgSize;
            return this;
        }

        public Builder maxUncommittedSize(long maxUncommittedSize) {
            this.maxUncommittedSize = maxUncommittedSize;
            return this;
        }

        public Builder maxInflightMsgs(int maxInflightMsgs) {
            this.maxInflightMsgs = maxInflightMsgs;
            return this;
        }

        public Builder maxInflightBytes(long maxInflightBytes) {
            this.maxInflightBytes = maxInflightBytes;
            return this;
        }

        public RaftConfig build() {
            return new RaftConfig(
                    electionTimeout,
                    heartbeatTimeout,
                    electionProtocol,
                    proposalHandleMode,
                    readIndexMode,
                    leaderLivenessPolicy,
                    maxMsgSize,
                    maxUncommittedSize,
                    maxInflightMsgs,
                    maxInflightBytes
            );
        }
    }
}
