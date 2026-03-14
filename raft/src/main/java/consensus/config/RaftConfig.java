package consensus.config;

import consensus.protocol.policy.ElectionProtocol;
import consensus.protocol.policy.LeaderLivenessPolicy;
import consensus.protocol.policy.ProposalHandleMode;
import consensus.protocol.policy.ReadIndexMode;
import consensus.engine.ExecutionModel;
import consensus.storage.AppliableEntriesPolicy;

/**
 * Immutable configuration for the Raft protocol and its downstream
 * components (log, inflight tracking, etc.).
 *
 * <p>Implements all role-specific and component-specific config
 * interfaces so a single instance can be passed to any component
 * that needs configuration. If a {@code RaftConfig} instance exists
 * it is guaranteed to be valid - all validation is performed at
 * construction time.</p>
 *
 * <p>Use {@link #builder()} to create instances.</p>
 *
 * @param electionTimeout      base election timeout in ticks. Also used
 *                             as the candidate/pre-candidate election
 *                             round timeout and the learner lease
 *                             timeout
 * @param heartbeatTimeout     heartbeat interval in ticks. Must be
 *                             strictly less than {@code electionTimeout}
 * @param electionProtocol     single-phase or two-phase (pre-election)
 *                             election
 * @param proposalHandleMode   how non-leader nodes handle client
 *                             proposals
 * @param readIndexMode        how the leader confirms authority for
 *                             linearizable reads
 * @param leaderLivenessPolicy whether the leader verifies quorum
 *                             liveness
 * @param executionModel       sequential or pipelined persistence and
 *                             application
 * @param maxMsgSize           maximum byte size of a single
 *                             AppendEntries batch
 * @param maxUncommittedSize   maximum total byte size of uncommitted
 *                             entries on the leader
 * @param maxInflightMsgs      maximum number of inflight append
 *                             messages per peer
 * @param maxInflightBytes     maximum total byte size of inflight
 *                             entries per peer
 * @param maxApplyingEntriesSize maximum total byte size of entries
 *                             being applied at once
 */
public record RaftConfig(
        int electionTimeout,
        int heartbeatTimeout,
        ElectionProtocol electionProtocol,
        ProposalHandleMode proposalHandleMode,
        ReadIndexMode readIndexMode,
        LeaderLivenessPolicy leaderLivenessPolicy,
        ExecutionModel executionModel,
        long maxMsgSize,
        long maxUncommittedSize,
        int maxInflightMsgs,
        long maxInflightBytes,
        long maxApplyingEntriesSize
) implements LeaderConfig, FollowerConfig, CandidateConfig, LearnerConfig, RaftLogConfig, PeerInflightConfig {

    /**
     * Compact constructor - validates all invariants.
     *
     * @throws IllegalArgumentException if any constraint is violated
     */
    public RaftConfig {
        // Heartbeats must tick at a positive rate.
        if (heartbeatTimeout <= 0) {
            throw new IllegalArgumentException("heartbeatTimeout must be greater than 0");
        }
        // Election timeout must be strictly greater than heartbeat timeout
        // so followers receive multiple heartbeats per election cycle.
        // If they were equal, a single missed heartbeat would trigger an
        // election - far too sensitive.
        if (electionTimeout <= heartbeatTimeout) {
            throw new IllegalArgumentException(
                    "electionTimeout must be greater than heartbeatTimeout");
        }
        // At least one message must be allowed in flight per peer,
        // otherwise the leader can never send entries.
        if (maxInflightMsgs <= 0) {
            throw new IllegalArgumentException("maxInflightMsgs must be greater than 0");
        }
        // The byte budget must be large enough to fit at least one
        // maximum-sized message, otherwise the leader could never send
        // even a single batch.
        if (maxInflightBytes < maxMsgSize) {
            throw new IllegalArgumentException("maxInflightBytes must be >= maxMsgSize");
        }
        // Lease-based reads rely on the quorum-check mechanism to
        // provide an implicit lease window. Without it the leader has
        // no way to know its lease is still valid, making stale reads
        // possible.
        if (readIndexMode == ReadIndexMode.LEASE && leaderLivenessPolicy != LeaderLivenessPolicy.QUORUM_VERIFIED) {
            throw new IllegalArgumentException(
                    "leaderLivenessPolicy must be QUORUM_VERIFIED when readIndexMode is LEASE");
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Delegates to {@link #electionTimeout()} - candidates and
     * pre-candidates use the same base timeout as followers.</p>
     *
     * @return the election timeout
     */
    @Override
    public int electionRoundTimeout() {
        return electionTimeout;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Delegates to {@link #electionTimeout()} - learners use the
     * election timeout as their lease window.</p>
     *
     * @return the election timeout
     */
    @Override
    public int leaseTimeout() {
        return electionTimeout;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Derived from {@link #executionModel()}:
     * {@link ExecutionModel#SEQUENTIAL} maps to
     * {@link AppliableEntriesPolicy#COMMITTED},
     * {@link ExecutionModel#PIPELINED} maps to
     * {@link AppliableEntriesPolicy#PERSISTED_COMMITTED}.</p>
     *
     * @return the appliable entries policy
     */
    @Override
    public AppliableEntriesPolicy appliableEntriesPolicy() {
        return switch (executionModel) {
            case ExecutionModel.SEQUENTIAL -> AppliableEntriesPolicy.COMMITTED;
            case ExecutionModel.PIPELINED -> AppliableEntriesPolicy.PERSISTED_COMMITTED;
        };
    }

    /**
     * Creates a new builder with sensible defaults.
     *
     * <p>Defaults:</p>
     * <ul>
     *   <li>{@code electionProtocol} = {@link ElectionProtocol#DUAL_ELECTION}</li>
     *   <li>{@code proposalHandleMode} = {@link ProposalHandleMode#FORWARD_TO_LEADER}</li>
     *   <li>{@code readIndexMode} = {@link ReadIndexMode#HEARTBEAT_IMMEDIATE}</li>
     *   <li>{@code leaderLivenessPolicy} = {@link LeaderLivenessPolicy#UNMONITORED}</li>
     *   <li>{@code maxMsgSize}, {@code maxUncommittedSize}, {@code maxInflightBytes} = unbounded</li>
     * </ul>
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link RaftConfig}. All timeout and inflight fields
     * must be set explicitly; policy fields have defaults.
     */
    public static final class Builder {
        private int electionTimeout;
        private int heartbeatTimeout;
        private ElectionProtocol electionProtocol;
        private ProposalHandleMode proposalHandleMode;
        private ReadIndexMode readIndexMode;
        private LeaderLivenessPolicy leaderLivenessPolicy;
        private ExecutionModel executionModel;
        private long maxMsgSize;
        private long maxUncommittedSize;
        private int maxInflightMsgs;
        private long maxInflightBytes;
        private long maxApplyingEntriesSize;

        private Builder() {
            this.electionProtocol = ElectionProtocol.DUAL_ELECTION;
            this.proposalHandleMode = ProposalHandleMode.FORWARD_TO_LEADER;
            this.readIndexMode = ReadIndexMode.HEARTBEAT_IMMEDIATE;
            this.leaderLivenessPolicy = LeaderLivenessPolicy.UNMONITORED;
            this.maxMsgSize = Long.MAX_VALUE;
            this.maxUncommittedSize = Long.MAX_VALUE;
            this.maxInflightBytes = Long.MAX_VALUE;
        }

        /** @param electionTimeout base election timeout in ticks
         *  @return this builder */
        public Builder electionTimeout(int electionTimeout) {
            this.electionTimeout = electionTimeout;
            return this;
        }

        /** @param heartbeatTimeout heartbeat interval in ticks
         *  @return this builder */
        public Builder heartbeatTimeout(int heartbeatTimeout) {
            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        /** @param electionProtocol election protocol to use
         *  @return this builder */
        public Builder electionProtocol(ElectionProtocol electionProtocol) {
            this.electionProtocol = electionProtocol;
            return this;
        }

        /** @param proposalHandleMode how non-leaders handle proposals
         *  @return this builder */
        public Builder proposalHandleMode(ProposalHandleMode proposalHandleMode) {
            this.proposalHandleMode = proposalHandleMode;
            return this;
        }

        /** @param readIndexMode linearizable read confirmation mode
         *  @return this builder */
        public Builder readIndexMode(ReadIndexMode readIndexMode) {
            this.readIndexMode = readIndexMode;
            return this;
        }

        /** @param leaderLivenessPolicy leader liveness policy
         *  @return this builder */
        public Builder leaderLivenessPolicy(LeaderLivenessPolicy leaderLivenessPolicy) {
            this.leaderLivenessPolicy = leaderLivenessPolicy;
            return this;
        }

        /** @param maxMsgSize maximum byte size of a single AppendEntries batch
         *  @return this builder */
        public Builder maxMsgSize(long maxMsgSize) {
            this.maxMsgSize = maxMsgSize;
            return this;
        }

        /** @param maxUncommittedSize maximum total uncommitted entry bytes
         *  @return this builder */
        public Builder maxUncommittedSize(long maxUncommittedSize) {
            this.maxUncommittedSize = maxUncommittedSize;
            return this;
        }

        /** @param maxInflightMsgs maximum inflight messages per peer
         *  @return this builder */
        public Builder maxInflightMsgs(int maxInflightMsgs) {
            this.maxInflightMsgs = maxInflightMsgs;
            return this;
        }

        /** @param maxInflightBytes maximum inflight bytes per peer
         *  @return this builder */
        public Builder maxInflightBytes(long maxInflightBytes) {
            this.maxInflightBytes = maxInflightBytes;
            return this;
        }

        /** @param executionModel sequential or pipelined execution
         *  @return this builder */
        public Builder executionModel(ExecutionModel executionModel) {
            this.executionModel = executionModel;
            return this;
        }

        /** @param maxApplyingEntriesSize maximum applying entry bytes
         *  @return this builder */
        public Builder maxApplyingEntriesSize(long maxApplyingEntriesSize) {
            this.maxApplyingEntriesSize = maxApplyingEntriesSize;
            return this;
        }

        /**
         * Builds and validates the {@link RaftConfig}.
         *
         * @return the validated config
         * @throws IllegalArgumentException if any constraint is violated
         */
        public RaftConfig build() {
            return new RaftConfig(
                    electionTimeout,
                    heartbeatTimeout,
                    electionProtocol,
                    proposalHandleMode,
                    readIndexMode,
                    leaderLivenessPolicy,
                    executionModel,
                    maxMsgSize,
                    maxUncommittedSize,
                    maxInflightMsgs,
                    maxInflightBytes,
                    maxApplyingEntriesSize
            );
        }
    }
}
