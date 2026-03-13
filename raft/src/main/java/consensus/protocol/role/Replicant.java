package consensus.protocol.role;

import consensus.core.NodeId;

public sealed interface Replicant extends Role permits Follower, Learner {
    boolean hasLeader();
    NodeId leaderId();
    void setLeader(NodeId id);
    void forgetLeader();
}
