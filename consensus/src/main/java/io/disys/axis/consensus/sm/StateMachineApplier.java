package io.disys.axis.consensus.sm;

import io.disys.axis.consensus.model.RaftPayload;
import io.disys.jaft.node.task.ApplyTask;

import java.util.concurrent.BlockingQueue;

public class StateMachineApplier {
    private final BlockingQueue<ApplyTask<RaftPayload>> tasks;

    public StateMachineApplier(BlockingQueue<ApplyTask<RaftPayload>> tasks) {
        this.tasks = tasks;
    }
}
