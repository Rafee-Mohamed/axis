package consensus.node;

import consensus.storage.Entry;
import consensus.storage.Payload;

import java.util.List;

public final class ApplyTask<T extends Payload> implements CompletableTask {
    private final List<T> entriesToApply;
    private final Runnable onComplete;

    @SuppressWarnings("unchecked")
    public ApplyTask(List<Entry.Data> entriesToApply, Runnable onComplete) {
        this.entriesToApply = entriesToApply.stream().map(d -> (T) d.data()).toList();
        this.onComplete = onComplete;
    }

    @Override
    public void complete() {
        onComplete.run();
    }

    public List<T> entriesToApply() {
        return entriesToApply;
    }
}
