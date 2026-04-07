package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

import java.util.Optional;

public class KeyTimelineView {

    private final Optional<DeadSpansView> dsv;
    private final Optional<LiveSpanView> lsv;

    private KeyTimelineView(
            Optional<DeadSpansView> dsv,
            Optional<LiveSpanView> lsv
    ) {
        this.dsv = dsv;
        this.lsv = lsv;
    }

    record DeadSpansView(VolatileList<DeadSpan>.PinnedView deadSpans, int spanFrom, int spanTo, int revFrom, int revTo) {

        int revFromIdx(int floor) {
            return spanFrom == floor ? revFrom : 0;
        }

        int revToIdx(int floor) {
            return spanTo == floor ? revTo : deadSpans.get(floor).size() - 1;
        }

        Revision floor() {
            return deadSpans.get(spanTo).get(revTo);
        }

        Optional<Revision> floor(long commitSeq) {
            var spanFloor = Query.floorDeadSpan(deadSpans, spanFrom, spanTo, commitSeq);
            if (spanFloor < 0) {
                return Optional.empty();
            }
            var span = deadSpans.get(spanFloor);

            var floor = Query.floorRevision(span.revisions(), revFromIdx(spanFloor), revToIdx(spanFloor), commitSeq);

            if (floor < 0) {
                return Optional.empty();
            }

            return Optional.of(span.get(floor));
        }

    }
    record LiveSpanView(VolatileList<Revision>.PinnedView liveSpan, int from, int to) {
        Revision floor() {
            return liveSpan.get(to);
        }

        Optional<Revision> floor(long commitSeq) {
            var floor = Query.floorRevision(liveSpan, from, to, commitSeq);
            if (floor < 0) {
                return Optional.empty();
            }

            return Optional.of(liveSpan.get(floor));
        }
    }

    // Views shouldn't be empty. position must be valid within view. firstCommitSeq <= lastCommitSeq
    // Checked by KeyTimeline

    static Optional<KeyTimelineView> pinDead(VolatileList<DeadSpan>.PinnedView deadSpans, int position, long firstCommitSeq, long lastCommitSeq) {
        return deadSpansView(deadSpans, position, firstCommitSeq, lastCommitSeq)
                .map(dsv -> new KeyTimelineView(Optional.of(dsv), Optional.empty()));
    }

    static Optional<KeyTimelineView> pinLive(VolatileList<Revision>.PinnedView liveSpan, long firstCommitSeq, long lastCommitSeq) {
        return liveSpanView(liveSpan, firstCommitSeq, lastCommitSeq)
                .map(lsv -> new KeyTimelineView(Optional.empty(), Optional.of(lsv)));
    }

    static Optional<KeyTimelineView> pin(
            VolatileList<DeadSpan>.PinnedView deadSpans,
            VolatileList<Revision>.PinnedView liveSpan,
            long firstCommitSeq,
            long lastCommitSeq
    ) {

        var dsv = deadSpansView(deadSpans, deadSpans.size() - 1, firstCommitSeq, lastCommitSeq);
        var lsv = liveSpanView(liveSpan, firstCommitSeq, lastCommitSeq);

        if (dsv.isEmpty() && lsv.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new KeyTimelineView(dsv, lsv));

    }
    
    static Optional<LiveSpanView> liveSpanView(VolatileList<Revision>.PinnedView liveSpan, long firstCommitSeq, long lastCommitSeq) {
        var revFrom = Query.lowerBoundRevision(liveSpan, firstCommitSeq);

        if (revFrom >= liveSpan.size()) {
            return Optional.empty();
        }

        var revTo = Query.floorRevision(liveSpan, lastCommitSeq);

        if (revTo < 0 || revFrom > revTo) {
            return Optional.empty();
        }
        
        return Optional.of(new LiveSpanView(liveSpan, revFrom, revTo));
    }
    
    static Optional<DeadSpansView> deadSpansView(VolatileList<DeadSpan>.PinnedView deadSpans, int position, long firstCommitSeq, long lastCommitSeq) {
        var spanFrom = Query.lowerBoundDeadSpan(deadSpans, position, firstCommitSeq);
        if (spanFrom > position) {
            return Optional.empty();
        }

        var spanTo = Query.floorDeadSpan(deadSpans, position, lastCommitSeq);

        // Case: spanFrom > spanTo


        // spans [5, 8]          [25, 30]
        //              [10, 20] (firstCommitSeq, lastCommitSeq)

        // Span 0: revisions live in [5, 8] -> ends before F = 10.
        // Span 1: revisions live in [25, 30] -> starts after L = 20.
        // Window [10, 20] intersects no span.
        //
        //
        // lowerBound / spanFrom: Span 0 has last = 8 < 10. Span 1 has last = 30 >= 10 -> spanFrom = 1.
        // floor / spanTo: Span 0 has first = 5 <= 20. Span 1 has first = 25 > 20 -> last index with first <= L is 0 -> spanTo = 0.
        // So spanFrom = 1 > 0 = spanTo, even though 10 <= 20.

        if (spanTo < 0 || spanFrom > spanTo) {
            return Optional.empty();
        }

        var revFrom = Query.lowerBoundRevision(deadSpans.get(spanFrom).revisions(), firstCommitSeq);
        var revTo = Query.floorRevision(deadSpans.get(spanTo).revisions(), lastCommitSeq);



        // spans [5, 8             25, 30] -> spanFrom/spanTo same span
        //               [10, 20] (firstCommitSeq, lastCommitSeq)
        //
        // on the same span - the window can be between two revisions
        if (spanFrom == spanTo && revFrom > revTo) {
            return Optional.empty();
        }
        
        return Optional.of(new DeadSpansView(deadSpans, spanFrom, spanTo, revFrom, revTo));
    }
    

    public Revision floor() {
        return lsv.map(LiveSpanView::floor)
                .or(() -> dsv.map(DeadSpansView::floor))
                .get();
    }

    public Optional<Revision> floor(long commitSeq) {
        return lsv.flatMap(lsv -> lsv.floor(commitSeq))
                .or(() -> dsv.flatMap(dsv -> dsv.floor(commitSeq)));

    }
}
