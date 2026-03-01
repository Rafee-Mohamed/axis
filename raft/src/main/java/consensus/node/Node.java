package consensus.node;

import consensus.algorithm.Raft;
import consensus.message.Message;

import java.util.concurrent.BlockingQueue;

public class Node implements RaftNode {
    private BlockingQueue<RaftOutput> outputQueue;
    private BlockingQueue<RaftInput> inputQueue;
    private Raft raft;
    @Override
    public void tick() {

    }

    @Override
    public void startElection() {

    }

    @Override
    public void propose(byte[] data) {

    }

    @Override
    public void step(Message message) {

    }

    @Override
    public BlockingQueue<RaftOutput> getOutputQueue() {
        return null;
    }

    @Override
    public void advance() {

    }


    @Override
    public void transferLeadership(NodeId leader, NodeId transferee) {

    }

    @Override
    public void readIndex() {

    }

    @Override
    public void reportUnreachable() {

    }

    @Override
    public void reportSnapshot() {

    }

    @Override
    public void run() {

    }
}
