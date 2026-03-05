package consensus.node;

import consensus.storage.Entry;

import java.util.List;

public final class ApplyTask implements CompletableTask {
    private final List<Entry> entriesToApply;
    private final Runnable onComplete;

    public ApplyTask(List<Entry> entriesToApply, Runnable onComplete) {
        this.entriesToApply = entriesToApply;
        this.onComplete = onComplete;
    }

    @Override
    public void complete() {
        onComplete.run();
    }

    public List<Entry> entriesToApply() {
        return entriesToApply;
    }
}
