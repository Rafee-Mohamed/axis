package consensus.algorithm;

import consensus.config.RaftConfig;
import consensus.message.Message;
import consensus.node.NodeId;
import consensus.storage.RaftLog;

import java.util.List;

public class Raft {
    private NodeId id;
    private long term;
    private NodeId votedFor;

    private long commitIndex;
    private long lastApplied;

    private Role role;

    private NodeId leader;
    private NodeId transferee;

    private long currentConfigIndex;

    private long uncommittedEntriesSize;

    private long electionElapsed;
    private long heartbeatElapsed;
    private long heartbeatTimeout;
    private long electionTimeout;
    private long randomizedElectionTimeout;

    private List<Message> messages;
    private List<Message> messagesAfterAppend;
    private List<Message> pendingReadIndexMessages;
    private RaftLog log;

    static class Builder {
        private Raft raft;
        Builder() {
            raft = new Raft();
        }

        Builder id(long id) { raft.id = new NodeId(id); return this; }
        Builder term(long term) { raft.term = term; return this; }

        Raft build() {
            return raft;
        }

    }

    private Raft() {}

    private Raft(NodeId id) {

    }

    public static Builder builder() {
        return new Builder();
    }

    public static Raft fromConfig(RaftConfig config) {
        return builder().build();
    }
}





