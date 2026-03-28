package io.disys.axis.storage.mvcc;

import java.util.Optional;

public class ArrayKeyTimeline {
    private volatile Revision[][] deadSpans;
    private volatile int deadSpanCount;
    private volatile Revision[] liveSpan;
    private volatile int liveRevisionCount;

    private ArrayKeyTimeline(Revision[][] deadSpans, int deadSpanCount, Revision[] liveSpan, int liveRevisionCount) {
        this.deadSpans = deadSpans;
        this.deadSpanCount = deadSpanCount;
        this.liveSpan = liveSpan;
        this.liveRevisionCount = liveRevisionCount;
    }

    static ArrayKeyTimeline init(Revision revision) {
        var deadSpans = new Revision[100][];
        var revisions = new Revision[100];
        revisions[0] = revision;
        return new ArrayKeyTimeline(deadSpans, 0, revisions, 1);
    }

    static ArrayKeyTimeline fromLiveSpan(Revision[] revisions, int liveRevisionCount) {
        var deadSpans = new Revision[100][];
        return new ArrayKeyTimeline(deadSpans, 0, revisions, liveRevisionCount);
    }

    static ArrayKeyTimeline fromDeadSpans(Revision[][] deadSpans, int deadSpanCount) {
        var revisions = new Revision[100];
        return new ArrayKeyTimeline(deadSpans, deadSpanCount, revisions, 0);
    }

    static ArrayKeyTimeline fromTimeline(
            Revision[][] deadSpans,
            int deadSpanCount,
            Revision[] revisions,
            int liveRevisionCount) {
        return new ArrayKeyTimeline(deadSpans, deadSpanCount, revisions, liveRevisionCount);
    }



    void add(Revision revision) {
        resizeLive();
        liveSpan[liveRevisionCount] = revision;
        liveRevisionCount++;
    }

    void resizeLive() {
        if (liveRevisionCount < liveSpan.length) {
            return;
        }

        var newLiveSpan = new Revision[liveSpan.length * 2];
        System.arraycopy(liveSpan, 0, newLiveSpan, 0, liveRevisionCount);
        liveSpan = newLiveSpan;
    }

    void resizeDead() {
        if (deadSpanCount < deadSpans.length) {
            return;
        }

        var newDeadSpans = new Revision[deadSpans.length * 2][];
        System.arraycopy(deadSpans, 0, newDeadSpans, 0, deadSpans.length);
        deadSpans = newDeadSpans;
    }

    void complete(Revision revision) {
        if (liveRevisionCount == 0) {
            throw new IllegalStateException("No alive span to complete");
        }

        var deadSpan = new Revision[liveRevisionCount + 1];
        System.arraycopy(liveSpan, 0, deadSpan, 0, liveRevisionCount);
        deadSpan[liveRevisionCount] = revision;

        resizeDead();

        deadSpans[deadSpanCount] = deadSpan;
        deadSpanCount++;
        liveRevisionCount = 0;
        liveSpan = new Revision[100];
    }

    Revision firstRevision() {
        if (liveRevisionCount == 0) {
            return deadSpans[0][0];
        }

        return liveSpan[0];
    }

    Revision lastRevision() {
        if (deadSpanCount == 0) {
            return liveSpan[liveRevisionCount - 1];
        }
        var lastSpan = deadSpans[deadSpanCount - 1];
        return lastSpan[lastSpan.length - 1];
    }

    public int lowerBound(long commitSeq) {
        var left = 0;
        var right = deadSpanCount - 1;

        while (left <= right) {
            var mid = left + (right - left) / 2;
            // first revision
            var rev = deadSpans[mid][0];

            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        if (left <= 0 || left >= deadSpanCount) {
            return left;
        }

        var previousRev = deadSpans[left - 1];
        if (previousRev[previousRev.length - 1].compareTo(commitSeq) >= 0) {
            return left - 1;
        }

        return left;
    }

    int lowerBound(Revision[] revisions, int size, long commitSeq) {
        var left = 0;
        var right = size - 1;

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var rev = revisions[mid];

            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    Revision[] copy(Revision[] revisions, int from, int to, int size) {
        var copy = new Revision[size];
        System.arraycopy(revisions, from, copy, 0, to - from);
        return copy;
    }

    Revision[] compactDead(Revision[] revisions, int size, long commitSeq) {
        var lb = lowerBound(revisions, size, commitSeq);

        if (lb >= size) {
            throw new IllegalStateException("Compact dead shouldn't be empty");
        }

        if (lb == 0) {
            return revisions;
        }

        return copy(revisions, lb, size, size - lb);
    }

    Optional<ArrayKeyTimeline> compactLive(long commitSeq) {
        if (liveRevisionCount == 0) {
            throw new IllegalStateException("Timeline can't exists without a revision on live span if compacted");
        }

        var lb = lowerBound(liveSpan, liveRevisionCount, commitSeq);

        if (lb >= liveRevisionCount) {
            return Optional.empty();
        }

        if (lb == 0) {
            return Optional.of(
                    ArrayKeyTimeline.fromLiveSpan(liveSpan, liveRevisionCount));
        }

        var compacted = copy(liveSpan, lb, liveRevisionCount, liveSpan.length);
        // allocate, for live default size
        return Optional.of(ArrayKeyTimeline.fromLiveSpan(compacted, liveRevisionCount - lb));
    }

    public Optional<ArrayKeyTimeline> compact(long commitSeq) {
        if (commitSeq < firstRevision().commitSeq()) {
            return Optional.of(this);
        }

        if (commitSeq > lastRevision().commitSeq()) {
            return Optional.empty();
        }

        var spanIdx = deadSpanCount == 0 ? -1 : lowerBound(commitSeq);

        if (spanIdx < 0 || spanIdx >= deadSpanCount) {
            return compactLive(commitSeq);
        }

        var capacity = Math.max(deadSpans.length, 100);
        var uncompacted = new Revision[capacity][];

        var compactedDeadSpan = compactDead(deadSpans[spanIdx], deadSpans[spanIdx].length, commitSeq);

        uncompacted[0] = compactedDeadSpan;
        var newDeadSpanCount = deadSpanCount - spanIdx;
        spanIdx++;

        System.arraycopy(deadSpans, spanIdx, uncompacted, 1, deadSpanCount - spanIdx);

        if (liveRevisionCount == 0) {
            return Optional.of(ArrayKeyTimeline.fromDeadSpans(uncompacted, newDeadSpanCount));
        }

        // carrying the same ref shared between old and new index
        return Optional.of(ArrayKeyTimeline.fromTimeline(uncompacted, newDeadSpanCount, liveSpan, liveRevisionCount));
    }

}
