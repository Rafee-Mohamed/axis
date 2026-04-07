package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

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

    public void complete(Revision revision) {
        if (liveSpan.isEmpty()) {
            throw new IllegalStateException("No alive span to complete");
        }
        
        var nextDeadSpan = liveSpan.complete(revision);
        // publishing the next dead span first
        deadSpans.add(nextDeadSpan);
        // read on this interleaving would result in two same spans
        // present in both live and last dead span
        liveSpan = LiveSpan.empty(deadSpans.size());
    }

    public Revision firstRevision() {
        return deadSpans.isEmpty() ? liveSpan.firstRevision() : deadSpans.getFirst().firstRevision();
    }

    public Revision lastRevision() {
        return deadSpans.isEmpty() ? liveSpan.lastRevision() : deadSpans.getLast().lastRevision();
    }

    public KeySpan lastSpan() {
        return liveSpan.isEmpty() ? deadSpans.getLast() : liveSpan;
    }

    public Optional<KeySpan> liveSpan() {
        return liveSpan.isEmpty() ? Optional.empty() : Optional.of(liveSpan);
    }

    public Optional<KeyTimelineView> pin(long firstCommitSeq, long lastCommitSeq) {
        if (firstCommitSeq > lastCommitSeq) {
            throw new IllegalArgumentException("Invalid range");
        }
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

        var livePos = currLiveSpan.position();

        if (pinnedLiveSpan.isEmpty() && pinnedDeadSpans.isEmpty()) {
            throw new IllegalStateException("Inconsistent state: Timeline exists without a single span");
        }

        if (pinnedDeadSpans.isEmpty()) {
            return KeyTimelineView.pinLive(pinnedLiveSpan, firstCommitSeq, lastCommitSeq);
        }

        // live span is dead, so take up to position in dead span - including stale live span in dead spans
        if (pinnedDeadSpans.size() > livePos) {
            return KeyTimelineView.pinDead(pinnedDeadSpans, livePos, firstCommitSeq, lastCommitSeq);
        }

        // dead spans size == live span position - still live span is valid, so read up to all dead (livePos - 1), and the live span
        return KeyTimelineView.pin(pinnedDeadSpans, pinnedLiveSpan, firstCommitSeq, lastCommitSeq);
    }

    private Optional<KeyTimeline> compactLive(long commitSeq) {
        if (liveSpan.isEmpty()) {
            throw new IllegalStateException("Timeline can't exist without a revision on live span if compacted");
        }
        return liveSpan.compact(commitSeq).map(KeyTimeline::fromLiveSpan);
    }

    public Revision floor() {
        return liveSpan.isEmpty()
                ? deadSpans.getLast().lastRevision()
                : liveSpan.lastRevision();
    }

    public Optional<Revision> floor(long commitSeq) {
        var currLiveSpan = liveSpan;
        if (!currLiveSpan.isEmpty()) {
            var floor = Query.floorRevision(currLiveSpan.revisions(), commitSeq);
            if (floor >= 0) {
                return Optional.of(currLiveSpan.get(floor));
            }
        }

        if (deadSpans.isEmpty()) {
            return Optional.empty();
        }

        var spanFloor = Query.floorDeadSpan(deadSpans, commitSeq);

        if (spanFloor < 0) {
            return Optional.empty();
        }

        var revisions = deadSpans.get(spanFloor).revisions();
        var floor = Query.floorRevision(revisions, commitSeq);
        if (floor >= 0) {
            return Optional.of(revisions.get(floor));
        }

        return Optional.empty();
    }

    public Optional<KeyTimeline> compact(long commitSeq) {
        if (commitSeq < firstRevision().commitSeq()) {
            return Optional.of(this);
        }
        if (commitSeq > lastRevision().commitSeq()) {
            return Optional.empty();
        }
        int deadCount = deadSpans.size();
        int spanIdx = deadCount == 0 ? -1 : Query.lowerBoundDeadSpan(deadSpans, commitSeq);
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