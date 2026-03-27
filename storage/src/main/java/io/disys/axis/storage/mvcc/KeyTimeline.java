package io.disys.axis.storage.mvcc;

import java.util.*;

public class KeyTimeline {
    private final List<KeySpan> deadSpans;
    private KeySpan liveSpan;

    // key timeline should have at least on span either in dead spans or a live span
    // if no such span exists means empty timeline for no key which is not correct
    KeyTimeline(List<KeySpan> deadSpans, KeySpan liveSpan) {
        this.deadSpans = deadSpans;
        this.liveSpan = liveSpan;
    }

    static KeyTimeline init(Revision revision) {
        return new KeyTimeline(new ArrayList<>(), KeySpan.init(revision));
    }

    static KeyTimeline fromLiveSpan(KeySpan span) {
        return new KeyTimeline(new ArrayList<>(), span);
    }

    static KeyTimeline fromDeadSpans(List<KeySpan> deadSpans) {
        return new KeyTimeline(deadSpans, null);
    }

    static KeyTimeline fromTimeline(List<KeySpan> deadSpans, KeySpan liveSpan) {
        return new KeyTimeline(deadSpans, liveSpan);
    }


    KeySpan lastSpan() {
        // timeline can't exist without a single span - invariant. so always return a span.
        return liveSpan == null ? deadSpans.getLast() : liveSpan;
    }

    KeySpan firstSpan() {
        return deadSpans.isEmpty() ? liveSpan : deadSpans.getFirst();
    }

    Optional<KeySpan> liveSpan() {
        return Optional.ofNullable(liveSpan);
    }

    Revision lastRevision() {
        return lastSpan().lastRevision();
    }

    Revision firstRevision() {
       return firstSpan().firstRevision();
    }

    void add(Revision revision) {
        if (liveSpan == null) {
            liveSpan = KeySpan.init(revision);
        } else {
            liveSpan.add(revision);
        }
    }

    void complete(Revision revision) {
        if (liveSpan == null) {
            throw new IllegalStateException("No alive span to complete");
        }

        liveSpan.add(revision);
        deadSpans.add(liveSpan);
        liveSpan = null;
    }

    public int lowerBound(long commitSeq) {
        var left = 0;
        var right = deadSpans.size() - 1;

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var rev = deadSpans.get(mid).firstRevision();

            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        if (left <= 0 || left >= deadSpans.size()) {
             return left;
        }

        var previousRev = deadSpans.get(left - 1).lastRevision();
        if (previousRev.compareTo(commitSeq) >= 0) {
            return left - 1;
        }

        return left;
    }

    public Optional<KeyTimeline> compact(long commitSeq) {
        if (commitSeq < firstRevision().commitSeq()) {
            return Optional.of(this);
        }

        if (commitSeq > lastRevision().commitSeq()) {
            return Optional.empty();
        }

        var spanIdx = deadSpans.isEmpty() ? -1 : lowerBound(commitSeq);

        if (spanIdx < 0 || spanIdx >= deadSpans.size()) {
            return Optional.ofNullable(liveSpan)
                    .flatMap(span -> span.compact(commitSeq))
                    .map(KeyTimeline::fromLiveSpan);
        }

        var compactedDeadSpans = new ArrayList<KeySpan>();

        deadSpans.get(spanIdx)
                .compact(commitSeq)
                .ifPresent(compactedDeadSpans::add);

        compactedDeadSpans.addAll(deadSpans.subList(spanIdx + 1, deadSpans.size()));

        if (liveSpan == null) {
            return Optional.of(KeyTimeline.fromDeadSpans(compactedDeadSpans));
        }

        return Optional.of(KeyTimeline.fromTimeline(compactedDeadSpans, liveSpan));
    }
}
