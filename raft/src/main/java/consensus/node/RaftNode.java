package consensus.node;

import consensus.membership.MembershipChange;
import consensus.message.Message;

import java.util.concurrent.BlockingQueue;

public interface RaftNode extends Runnable {

    void tick();
    void startElection();
    void propose(byte[] data);
    void propose(MembershipChangeConfig config);
    void step(Message message);
    BlockingQueue<RaftOutput> getOutputQueue();
    void advance();
    void apply(MembershipChangeConfig config);
    void transferLeadership(NodeId leader, NodeId transferee);
    void readIndex();
    void reportUnreachable();
    void reportSnapshot();
}
