package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;

import java.util.List;
import java.util.Optional;

public class KeyTimeline {
    private final VolatileList<DeadSpan> deadSpans;
    private volatile LiveSpan liveSpan;

    private KeyTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        this.deadSpans = deadSpans;
        this.liveSpan = liveSpan;
    }

    public static KeyTimeline init(Revision revision) {
        return new KeyTimeline(
                VolatileList.allocate(10),
                LiveSpan.init(revision)
        );
    }

    public static KeyTimeline restore(Revision revision, Record record) {
        if (record.tombstone()) {
            return new KeyTimeline(
                    VolatileList.of(new DeadSpan(List.of(revision), record.createdAtSeq(), record.version())),
                    LiveSpan.empty(1)
            );
        }
        return new KeyTimeline(
                VolatileList.allocate(10),
                LiveSpan.restore(revision, record.createdAtSeq(), record.version())
        );
    }

    public static KeyTimeline fromLiveSpan(LiveSpan liveSpan) {
        return new KeyTimeline(VolatileList.allocate(10), liveSpan);
    }

    public static KeyTimeline fromDeadSpans(VolatileList<DeadSpan> deadSpans) {
        return new KeyTimeline(deadSpans, LiveSpan.empty(deadSpans.size()));
    }

    public static KeyTimeline fromTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        return new KeyTimeline(deadSpans, liveSpan);
    }

    public void add(Revision revision) {
        liveSpan.add(revision);
    }

    public boolean tryComplete(Revision revision) {
        if (liveSpan.isEmpty()) {
            return false;
        }

        var nextDeadSpan = liveSpan.complete(revision);
        // publishing the next dead span first
        deadSpans.add(nextDeadSpan);
        // read on this interleaving would result in two same spans
        // present in both live and last dead span
        liveSpan = LiveSpan.empty(deadSpans.size());

        return true;
    }


    public Optional<Revision> revisionAt(long commitSeq) {
        if (!liveSpan.isEmpty()) {
            var floor = Query.floorRevision(liveSpan.revisions(), commitSeq);
            if (floor >= 0) {
                return Optional.of(liveSpan.get(floor));
            }

            if (deadSpans.isEmpty()) {
                return Optional.empty();
            }
        }

        var spanFloor = Query.floorDeadSpan(deadSpans, commitSeq);

        if (spanFloor < 0) {
            return Optional.empty();
        }

        var span = deadSpans.get(spanFloor);
        var floor = Query.floorRevision(span.revisions(), commitSeq);

        // if the floor is last and the requested commitSeq is after the last revision commit seq
        // then the there is no revision at the requested commitSeq it is deleted, if the requested
        // commitSeq is same last revision then we can return the tombstone revision
        if (floor == span.revisions().size() - 1 && span.lastRevision().compareTo(commitSeq) < 0) {
            return Optional.empty();
        }

        return Optional.of(span.get(floor));
    }

    private Optional<Revision> revisionAt(
            VolatileList<DeadSpan>.PinnedView deadSpans,
            VolatileList<Revision>.PinnedView liveSpan,
            int position,
            long commitSeq
    ) {
        if (!liveSpan.isEmpty()) {
            var floor = Query.floorRevision(liveSpan, commitSeq);
            if (floor >= 0) {
                return Optional.of(liveSpan.get(floor));
            }
        }

        if (deadSpans.isEmpty()) {
            return Optional.empty();
        }

        var spanFloor = Query.floorDeadSpan(deadSpans, position, commitSeq);

        if (spanFloor < 0) {
            return Optional.empty();
        }

        var span = deadSpans.get(spanFloor);
        var floor = Query.floorRevision(span.revisions(), commitSeq);

        // if the floor is last and the requested commitSeq is after the last revision commit seq
        // then the there is no revision at the requested commitSeq it is deleted, if the requested
        // commitSeq is same last revision then we can return the tombstone revision
        if (floor == span.revisions().size() - 1 && span.lastRevision().compareTo(commitSeq) < 0) {
            return Optional.empty();
        }

        return Optional.of(span.get(floor));
    }

    public Optional<Revision> pinnedRevisionAt(long commitSeq) {
        /*
         * The livespan acts as the boundary for pinning, and its position represents the
         * final point up to which a reader needs to consider data.
         * The reasoning is based on how timeline reads work - from the reader’s perspective,
         * the currently observed livespan is the consistent view, and anything beyond that
         * is not required for correctness.  The reader only needs to consider data up to this point,
         * even though the timeline may have advanced further.
         *
         * This is tied to bound.end() - lastVisibleCommitSeq to reader, which acts as the bound.
         * The writer advances the commitSeq using a volatile write, and before advancing it,
         * all index mutations (put/delete operations) are already applied.
         * On the reader side, the commitSeq is read via a volatile read during livespan creation,
         * which guarantees that all mutations up to that commit sequence are visible.
         * There are no guarantees beyond that point, and the reader does not need them either.
         * So, if a livespan is observed through a volatile read, it is guaranteed to reflect a
         * state consistent up to that commit sequence - if the key exists at that point.
         *
         * Once a livespan is read, its position is final and refers to its index within deadSpans.
         * At the time of creation, this position effectively corresponds to deadSpans.size.
         * However, due to concurrency, the reader thread may get paused (for example, due to scheduling or GC),
         * and in the meantime, the writer may advance the timeline by adding more spans.
         * As a result, the livespan that was read as live may have already transitioned into a dead span
         * by the time the reader resumes, and the current deadSpans.size (read via volatile)
         * may now be greater than the position of that livespan.
         *
         * Based on this, the position acts as a stable boundary. If the position of the read livespan
         * is equal to the current deadSpans.size, it means the livespan is still live,
         * so for pinning we consider both all dead spans up to the current size and the current livespan.
         * If the deadSpans.size is greater than the livespan’s position, it means the livespan has already moved to dead,
         * so we can safely ignore anything beyond that position and only consider dead spans up to upto livespan's position.
         * i.e. the dead span of live span is included for view
         *
         * This ensures that the reader operates within a consistent boundary
         * even in the presence of concurrent timeline advancement.
         */
        // pinning the ref - has to do two ops - pin and position, so pin the ref
        // first and do the ops as required
        var currLiveSpan = liveSpan;

        // pin the current view of live span
        var pinnedLiveSpan = currLiveSpan.pin();

        // -- On this interleaving - the dead spans might have been advanced
        // i.e. the live span is dead by the time execution reaches this point
        // so pinnedDeadSpans may contain spans beyond liveSpan position
        //
        // pin the current dead spans it is, this pins the current size as well
        var pinnedDeadSpans = deadSpans.pin();

        if (pinnedLiveSpan.isEmpty() && pinnedDeadSpans.isEmpty()) {
            throw new IllegalStateException("Inconsistent state: Timeline exists without a single span");
        }


        return revisionAt(pinnedDeadSpans, pinnedLiveSpan, currLiveSpan.position(), commitSeq);
    }

    public Revision firstRevision() {
        return deadSpans.isEmpty() ? liveSpan.firstRevision() : deadSpans.getFirst().firstRevision();
    }

    public KeySpan lastSpan() {
        return deadSpans.isEmpty() ? liveSpan : deadSpans.getLast();
    }

    public Revision lastRevision() {
        return deadSpans.isEmpty() ? liveSpan.lastRevision() : deadSpans.getLast().lastRevision();
    }

    public Optional<KeyTimeline> compact(long commitSeq) {
        if (commitSeq <= firstRevision().commitSeq()) {
            return Optional.of(this);
        }

        if (deadSpans.isEmpty() && liveSpan.isEmpty()) {
            throw new IllegalStateException("Timeline can't exist without a revision in live or dead spans");
        }

        if (deadSpans.isEmpty()) {
            return Optional.of(fromLiveSpan(liveSpan.compact(commitSeq, 0)));
        }


        // spanFloor won't be -1 as first revision check is done at start
        var spanFloor = Query.floorDeadSpan(deadSpans, commitSeq);

        var compactedDeadSpan = deadSpans.get(spanFloor).compact(commitSeq);
        // if the dead span is last one and no dead span from compaction then
        // the commitSeq appears after the last revision of last dead span
        // therefore, only take compacted live span if present
        var deadCount = deadSpans.size() - spanFloor - (compactedDeadSpan.isEmpty() ? 1 : 0);

        if (deadCount == 0) {
            if (liveSpan.isEmpty()) {
                // if no live span and no dead span survives, the drop the timeline
                return Optional.empty();
            }
            return Optional.of(fromLiveSpan(liveSpan.compact(commitSeq, 0)));
        }

        var uncompactedDeadSpans = VolatileList.<DeadSpan>allocate(deadCount);

        compactedDeadSpan.ifPresent(uncompactedDeadSpans::add);

        for (int i = spanFloor + 1; i < deadSpans.size(); i++) {
            uncompactedDeadSpans.stage(deadSpans.get(i));
        }

        uncompactedDeadSpans.publish();

        if (liveSpan.isEmpty()) {
            return Optional.of(fromDeadSpans(uncompactedDeadSpans));
        }
        return Optional.of(fromTimeline(
                uncompactedDeadSpans,
                // after compaction prefix of dead spans might be removed
                // therefore must update the position of live span
                // in compacted dead spans
                liveSpan.compact(commitSeq, uncompactedDeadSpans.size())
        ));
    }
}