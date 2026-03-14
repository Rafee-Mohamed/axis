package consensus.node.task;

import consensus.storage.Entry;
import consensus.storage.Payload;

import java.util.List;

/**
 * A task that carries committed data entries for state machine application.
 *
 * <p>The raw {@link Entry.Data} entries are unwrapped to their typed
 * {@link Payload} form so the application works with its own domain
 * types directly.</p>
 *
 * <p>Calling {@link #complete()} delivers the apply response back to
 * the engine, advancing the processing loop.</p>
 *
 * @param <T> the application's payload type
 */
public final class ApplyTask<T extends Payload> implements CompletableTask {

    private final List<T> entriesToApply;
    private final Runnable onComplete;

    /**
     * @param entriesToApply the committed data entries to apply
     * @param onComplete     callback that delivers the apply response
     *                       back to the engine
     */
    @SuppressWarnings("unchecked")
    public ApplyTask(List<Entry.Data> entriesToApply, Runnable onComplete) {
        this.entriesToApply = entriesToApply.stream().map(d -> (T) d.data()).toList();
        this.onComplete = onComplete;
    }

    @Override
    public void complete() {
        onComplete.run();
    }

    /** @return the typed payloads to apply to the state machine */
    public List<T> entriesToApply() {
        return entriesToApply;
    }
}
