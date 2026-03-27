package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class KeyTimeline {
    private final List<KeySpan> deadSpans;
    private KeySpan currentSpan;

    KeyTimeline(Revision revision) {
        deadSpans = new ArrayList<>();
        currentSpan = new KeySpan(revision);
    }

    KeySpan last() {
        // timeline can't exist without a single span - invariant. so always return a span.
        return currentSpan == null ? deadSpans.getLast() : currentSpan;
    }

    Optional<KeySpan> currentSpan() {
        return Optional.ofNullable(currentSpan);
    }

    Revision lastRevision() {
        return last().revision();
    }

    Revision firstRevision() {
       return deadSpans.getFirst().firstRevision();
    }

    void add(Revision revision) {
        if (currentSpan == null) {
            currentSpan = new KeySpan(revision);
        } else {
            currentSpan.add(revision);
        }
    }

    void complete(Revision revision) {
        if (currentSpan == null) {
            throw new IllegalStateException("No alive span to complete");
        }

        currentSpan.add(revision);
        deadSpans.add(currentSpan);
        currentSpan = null;
    }

    public Optional<KeyTimeline> compact(long commitSeq) {
        if (firstRevision().commitSeq() > commitSeq) {
            return Optional.empty();
        }

        return Optional.of(this);
    }
}
