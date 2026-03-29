package io.disys.axis.storage.mvcc;

import java.util.Optional;

public class VolatileListKeyTimeline {
    private final VolatileList<DeadSpan> deadSpans;
    private LiveSpan liveSpan;
    
    private VolatileListKeyTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        this.deadSpans = deadSpans;
        this.liveSpan = liveSpan;
    }

    static VolatileListKeyTimeline init(Revision revision) {
        return new VolatileListKeyTimeline(
                VolatileList.allocate(10),
                LiveSpan.init(revision)
        );
    }

    static VolatileListKeyTimeline fromLiveSpan(LiveSpan liveSpan) {
        return new VolatileListKeyTimeline(VolatileList.allocate(10), liveSpan);
    }

    static VolatileListKeyTimeline fromDeadSpans(VolatileList<DeadSpan> deadSpans) {
        return new VolatileListKeyTimeline(deadSpans, new LiveSpan(deadSpans.size(), VolatileList.allocate(10)));
    }

    static VolatileListKeyTimeline fromTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        return new VolatileListKeyTimeline(deadSpans, liveSpan);
    }

    void add(Revision revision) {
        liveSpan.add(revision);
    }

    void complete(Revision revision) {
        if (liveSpan.revisions().isEmpty()) {
            throw new IllegalStateException("No alive span to complete");
        }
        
        var nextDeadSpan = liveSpan.complete(revision);
        // publishing the next dead span first
        deadSpans.add(nextDeadSpan);
        // read on this interleaving would result in two same spans
        // present in both live and last dead span
        liveSpan = new LiveSpan(deadSpans.size(), VolatileList.allocate(10));
        // to refresh the live span ref
        liveSpan.release();
    }

    Revision firstRevision() {
        return deadSpans.isEmpty() ? liveSpan.firstRevision() : deadSpans.getFirst().firstRevision();
    }

    Revision lastRevision() {
        return deadSpans.isEmpty() ? liveSpan.lastRevision() : deadSpans.getLast().lastRevision();
    }

    Optional<KeySpanView> liveSpan() {
        return liveSpan.isEmpty() ? Optional.empty() : Optional.of(liveSpan);
    }

    private int lowerBoundDead(long commitSeq) {
        int n = deadSpans.size();
        int left = 0;
        int right = n - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            var rev = deadSpans.get(mid).firstRevision();
            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        if (left <= 0 || left >= n) {
            return left;
        }
        var previous = deadSpans.get(left - 1);
        if (previous.lastRevision().compareTo(commitSeq) >= 0) {
            return left - 1;
        }
        return left;
    }

    private int lowerBoundLive(long commitSeq) {
        int left = 0;
        int right = liveSpan.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            var rev = liveSpan.get(mid);
            if (rev.compareTo(commitSeq) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private Optional<VolatileListKeyTimeline> compactLive(long commitSeq) {
        if (liveSpan.isEmpty()) {
            throw new IllegalStateException("Timeline can't exist without a revision on live span if compacted");
        }
        return liveSpan.compact(commitSeq).map(VolatileListKeyTimeline::fromLiveSpan);
    }

    public Optional<VolatileListKeyTimeline> compact(long commitSeq) {
        if (commitSeq < firstRevision().commitSeq()) {
            return Optional.of(this);
        }
        if (commitSeq > lastRevision().commitSeq()) {
            return Optional.empty();
        }
        int deadCount = deadSpans.size();
        int spanIdx = deadCount == 0 ? -1 : lowerBoundDead(commitSeq);
        if (spanIdx < 0 || spanIdx >= deadCount) {
            return compactLive(commitSeq);
        }

        // uncompactedDeadSpans logical size == newDeadCount == (deadCount - spanIdx)
        var uncompactedDeadSpans = VolatileList.<DeadSpan>allocate(deadCount);

        var span = deadSpans.get(spanIdx);
        var compactedRow = span.compact(commitSeq).orElseThrow(
                () -> new IllegalStateException("Compact dead shouldn't be empty"));
        spanIdx++;

        uncompactedDeadSpans.stage(compactedRow);

        for (int i = spanIdx; i < deadCount; i++) {
            uncompactedDeadSpans.stage(deadSpans.get(i));
        }

        uncompactedDeadSpans.publish();

        if (liveSpan.isEmpty()) {
            return Optional.of(fromDeadSpans(uncompactedDeadSpans));
        }
        return Optional.of(fromTimeline(uncompactedDeadSpans, liveSpan));
    }
}