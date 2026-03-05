package consensus.node;

public sealed interface CompletableTask permits PersistTask, ApplyTask {
    void complete();
}
