package io.disys.axis.storage.mvcc;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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

        Optional<Revision> floor(Revision revision) {
            var spanFloor = KeyTimelineView.floor(idx -> deadSpans.get(idx).firstRevision(), spanFrom, spanTo, revision);
            if (spanFloor < 0) {
                return Optional.empty();
            }
            var span = deadSpans.get(spanFloor);

            var floor = KeyTimelineView.floor(span::get, revFromIdx(spanFloor), revToIdx(spanFloor), revision);

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

        Optional<Revision> floor(Revision revision) {
            var floor = KeyTimelineView.floor(liveSpan::get, from, to, revision);
            if (floor < 0) {
                return Optional.empty();
            }

            return Optional.of(liveSpan.get(floor));
        }
    }

    // Views shouldn't be empty. position must be valid within view. firstCommitSeq <= lastCommitSeq
    // Checked by KeyTimeline

    static Optional<KeyTimelineView> pinDead(VolatileList<DeadSpan>.PinnedView deadSpans, int position, long firstCommitSeq, long lastCommitSeq) {
        var firstRevision = Revision.create(firstCommitSeq);
        var lastRevision = new Revision(lastCommitSeq, 0);

        return deadSpansView(deadSpans, position, firstRevision, lastRevision)
                .map(dsv -> new KeyTimelineView(Optional.of(dsv), Optional.empty()));
    }

    static Optional<KeyTimelineView> pinLive(VolatileList<Revision>.PinnedView liveSpan, long firstCommitSeq, long lastCommitSeq) {
        var firstRevision = Revision.create(firstCommitSeq);
        var lastRevision = new Revision(lastCommitSeq, 0);

        return liveSpanView(liveSpan, firstRevision, lastRevision)
                .map(lsv -> new KeyTimelineView(Optional.empty(), Optional.of(lsv)));
    }

    static Optional<KeyTimelineView> pin(
            VolatileList<DeadSpan>.PinnedView deadSpans,
            VolatileList<Revision>.PinnedView liveSpan,
            long firstCommitSeq,
            long lastCommitSeq
    ) {

        var firstRevision = Revision.create(firstCommitSeq);
        var lastRevision = new Revision(lastCommitSeq, 0);

        var dsv = deadSpansView(deadSpans, deadSpans.size() - 1, firstRevision, lastRevision);
        var lsv = liveSpanView(liveSpan, firstRevision, lastRevision);

        if (dsv.isEmpty() && lsv.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new KeyTimelineView(dsv, lsv));

    }
    
    static Optional<LiveSpanView> liveSpanView(VolatileList<Revision>.PinnedView liveSpan, Revision firstRevision, Revision lastRevision) {
        var revFrom = lowerBoundRevisions(liveSpan, firstRevision);

        if (revFrom >= liveSpan.size()) {
            return Optional.empty();
        }

        var revTo = floorRevisions(liveSpan, lastRevision);

        if (revTo < 0 || revFrom > revTo) {
            return Optional.empty();
        }
        
        return Optional.of(new LiveSpanView(liveSpan, revFrom, revTo));
    }
    
    static Optional<DeadSpansView> deadSpansView(VolatileList<DeadSpan>.PinnedView deadSpans, int position, Revision firstRevision, Revision lastRevision) {
        var spanFrom = lowerBoundDeadSpans(deadSpans, position, firstRevision);
        if (spanFrom > position) {
            return Optional.empty();
        }

        var spanTo = floorDeadSpans(deadSpans, position, lastRevision);

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

        var revFrom = lowerBoundRevisions(deadSpans.get(spanFrom).revisions(), firstRevision);
        var revTo = floorRevisions(deadSpans.get(spanTo).revisions(), lastRevision);



        // spans [5, 8             25, 30] -> spanFrom/spanTo same span
        //               [10, 20] (firstCommitSeq, lastCommitSeq)
        //
        // on the same span - the window can be between two revisions
        if (spanFrom == spanTo && revFrom > revTo) {
            return Optional.empty();
        }
        
        return Optional.of(new DeadSpansView(deadSpans, spanFrom, spanTo, revFrom, revTo));
    }

    static <T extends Comparable<T>> int lowerBound(Function<Integer, T> get, int left, int right, T t) {

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var item = get.apply(mid);

            if (item.compareTo(t) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    static <T extends Comparable<T>> int floor(Function<Integer, T> get, int left, int right, T t) {

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var item = get.apply(mid);

            if (item.compareTo(t) <= 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return right;
    }

    static int lowerBoundDeadSpans(VolatileList<DeadSpan>.PinnedView deadSpans, int right, Revision revision) {
        return lowerBound(idx -> deadSpans.get(idx).lastRevision(), 0, right, revision);
    }


    static int floorDeadSpans(VolatileList<DeadSpan>.PinnedView deadSpans, int right, Revision revision) {
        return floor(idx -> deadSpans.get(idx).firstRevision(), 0, right, revision);
    }

    static int lowerBoundRevisions(VolatileList<Revision>.PinnedView revisions, Revision revision) {
        return lowerBound(revisions::get, 0, revisions.size() - 1, revision);
    }

    static int floorRevisions(VolatileList<Revision>.PinnedView revisions, Revision revision) {
        return floor(revisions::get, 0, revisions.size() - 1, revision);
    }

    static int lowerBoundRevisions(List<Revision> revisions, Revision revision) {
        return lowerBound(revisions::get, 0, revisions.size() - 1, revision);
    }

    static int floorRevisions(List<Revision> revisions, Revision revision) {
        return floor(revisions::get, 0, revisions.size() - 1, revision);
    }


    Optional<Revision> floor() {
        return lsv.map(LiveSpanView::floor)
                .or(() -> dsv.map(DeadSpansView::floor));
    }

    Optional<Revision> floor(long commitSeq) {
        var revision = Revision.create(commitSeq);

        return lsv.flatMap(lsv -> lsv.floor(revision))
                .or(() -> dsv.flatMap(dsv -> dsv.floor(revision)));

    }
}
