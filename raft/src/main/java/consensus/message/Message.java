package consensus.message;

import consensus.algorithm.Snapshot;
import consensus.storage.Entry;
import consensus.node.NodeId;
import consensus.membership.MembershipTransition;
import consensus.membership.MembershipChange;

import java.util.List;

/**
 * A unified interface for Messages - local triggers and network messages
 */
public sealed interface Message {

    // ═══════════════════════════════════════════════════════════════════════════
    // REPLICATION MESSAGES
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Leader → Follower: Replicate log entries.
     * Also used as heartbeat when entries is empty.
     * Raft paper: AppendEntries RPC
     */
    record AppendEntries(
            NodeId to,
            NodeId from,
            long term,              // leader's term
            long prevLogIndex,      // index of log entry immediately preceding new ones
            long prevLogTerm,       // term of prevLogIndex entry
            List<Entry> entries,    // log entries to store (empty for heartbeat)
            long leaderCommit       // leader's commitIndex
    ) implements Message {
    }

    /**
     * Follower → Leader: Response to AppendEntries.
     */
    record AppendEntriesResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean success,        // true if follower contained entry matching prevLogIndex/prevLogTerm
            long matchIndex,        // highest index known to be replicated (on success)
            long rejectHint,        // hint for leader to find correct nextIndex (on rejection)
            long rejectHintTerm     // term of the conflicting entry (for faster backtracking)
    ) implements Message {
    }

    /**
     * Leader → Follower: Heartbeat (lightweight, no entries).
     * Separate from AppendEntries for clarity, though semantically similar.
     */
    record Heartbeat(
            NodeId to,
            NodeId from,
            long term,
            long leaderCommit       // leader's commitIndex
    ) implements Message {
    }

    /**
     * Follower → Leader: Response to Heartbeat.
     */
    record HeartbeatResponse(
            NodeId to,
            NodeId from,
            long term
    ) implements Message {
    }

    /**
     * Leader → Follower: Install snapshot when follower is too far behind.
     * <p>
     * Raft paper: InstallSnapshot RPC
     */
    record InstallSnapshot(
            NodeId to,
            NodeId from,
            long term,
            Snapshot snapshot       // contains: lastIncludedIndex, lastIncludedTerm, data, config
    ) implements Message {
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ELECTION MESSAGES
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Candidate → All: Request vote in election.
     * <p>
     * Raft paper: RequestVote RPC
     */
    record RequestVote(
            NodeId to,
            NodeId from,
            long term,              // candidate's term
            long lastLogTerm ,     // term of candidate's last log entry
            long lastLogIndex     // index of candidate's last log entry
    ) implements Message {
    }

    /**
     * Voter → Candidate: Response to RequestVote.
     */
    record RequestVoteResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean voteGranted     // true if candidate received vote
    ) implements Message {
    }

    /**
     * PreCandidate → All: Pre-vote before incrementing term.
     * <p>
     * Prevents disruption from partitioned nodes.
     * Same fields as RequestVote.
     */
    record RequestPreVote(
            NodeId to,
            NodeId from,
            long term,              // would-be term (current + 1)
            long lastLogTerm,
            long lastLogIndex
    ) implements Message {
    }

    /**
     * Voter → PreCandidate: Response to PreVote.
     */
    record RequestPreVoteResponse(
            NodeId to,
            NodeId from,
            long term,
            boolean voteGranted
    ) implements Message {
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // LEADERSHIP TRANSFER MESSAGES
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Request to transfer leadership to a specific node.
     * Can be sent to leader from any node, or internally triggered.
     */
    record TransferLeadership(
            NodeId to,
            NodeId from,
            long term,
            NodeId transferee       // the node that should become leader
    ) implements Message {
    }

    /**
     * Leader → Transferee: Tell the transferee to start election immediately.
     * Sent after leader has caught up the transferee's log.
     */
    record TimeoutNow(
            NodeId to,
            NodeId from,
            long term
    ) implements Message {
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // READ-ONLY QUERY MESSAGES
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Follower → Leader: Request read index for linearizable read.
     */
    record ReadIndex(
            NodeId to,
            NodeId from,
            long term,
            byte[] context          // opaque context returned with response
    ) implements Message {
    }

    /**
     * Leader → Follower: Response with committed index for read.
     */
    record ReadIndexResponse(
            NodeId to,
            NodeId from,
            long term,
            long readIndex,         // commit index at time of request
            byte[] context          // echoed context
    ) implements Message {
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // LOCAL MESSAGES (internal triggers, not sent over network)
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Internal: Trigger election (from election timeout).
     */
    record TriggerElection(
            NodeId from             // self
    ) implements Message {
    }

    /**
     * Internal: Trigger heartbeat broadcast (leader's heartbeat tick).
     */
    record TriggerHeartbeat(
            NodeId from             // self (leader)
    ) implements Message {
    }

    /**
     * Internal: Proposal from application.
     */
    record Proposal(
            NodeId from,            // self
            byte[] data             // proposed data
    ) implements Message {
    }

    /**
     * Internal: Proposal for configuration change.
     */
    record ProposalMembershipChange(
            NodeId from,
            List<MembershipChange> changes,
            MembershipTransition transition,
            byte[] context
    ) implements Message {
    }

    /**
     * Internal: Check if leader still has quorum (CheckQuorum feature).
     */
    record CheckQuorum(
            NodeId from             // self (leader)
    ) implements Message {
    }
}
