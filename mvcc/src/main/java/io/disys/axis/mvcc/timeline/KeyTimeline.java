package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;

import java.util.List;
import java.util.Optional;

/**
 * The full revision history of a single key: an ordered sequence of
 * {@link DeadSpan}s followed by a {@link LiveSpan}.
 *
 * <pre>
 * [ dead[0] ] [ dead[1] ] ... [ dead[n-1] ] [   live   ]
 *  r1..r3(del) r5..r7(del)    r9..r11(del)   r13..
 *  pos=0       pos=1           pos=n-1         pos=n
 * </pre>
 *
 * <p>The live span may be empty when the key is currently deleted.
 * Each dead span's position equals its index in {@code deadSpans}; the
 * live span's position is pre-assigned as {@code deadSpans.size()} at
 * the time it is created.</p>
 *
 * <h2>Threading</h2>
 * <p>Single writer; multiple concurrent readers via {@link #getPinnedAt(long)}.
 * Writer-side reads use {@link #getAt(long)}, which is not thread-safe.</p>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>At least one non-empty span always exists.</li>
 *   <li>Dead spans are chronologically ordered; each span's last revision
 *       precedes the next span's first revision.</li>
 *   <li>{@code liveSpan} is never {@code null}; it may be empty.</li>
 * </ul>
 */
public class KeyTimeline {
    /** Completed spans in chronological order; {@link VolatileList} for safe concurrent reads. */
    private final VolatileList<DeadSpan> deadSpans;

    /**
     * The current active span; may be empty when the key is deleted.
     * Volatile so readers see the atomically replaced reference after {@link #tryComplete(Revision)}.
     */
    private volatile LiveSpan liveSpan;

    private KeyTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        this.deadSpans = deadSpans;
        this.liveSpan = liveSpan;
    }

    /** Creates a new timeline seeded with {@code revision} as the first entry. */
    public static KeyTimeline init(Revision revision) {
        return new KeyTimeline(
                VolatileList.allocate(10),
                LiveSpan.init(revision)
        );
    }

    /**
     * Rebuilds a timeline from persisted state. A tombstone record produces a
     * single dead span with an empty live span; a live record produces a live span.
     */
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

    static KeyTimeline fromLiveSpan(LiveSpan liveSpan) {
        return new KeyTimeline(VolatileList.allocate(10), liveSpan);
    }

    static KeyTimeline fromDeadSpans(VolatileList<DeadSpan> deadSpans) {
        return new KeyTimeline(deadSpans, LiveSpan.empty(deadSpans.size()));
    }

    static KeyTimeline fromTimeline(VolatileList<DeadSpan> deadSpans, LiveSpan liveSpan) {
        return new KeyTimeline(deadSpans, liveSpan);
    }

    /** Appends {@code revision} to the live span. Not thread-safe; for writer use only. */
    public void add(Revision revision) {
        liveSpan.add(revision);
    }

    /**
     * Closes the live span with {@code revision} as the deletion, moves it to
     * {@code deadSpans}, and replaces {@code liveSpan} with an empty span.
     *
     * <p>The dead span is published before {@code liveSpan} is replaced. During
     * this window a reader may observe the span in both; {@link #getPinnedAt(long)}
     * handles this safely via position-bounded search.</p>
     *
     * @return {@code false} if the live span was already empty
     */
    public boolean tryComplete(Revision revision) {
        if (liveSpan.isEmpty()) {
            return false;
        }

        var nextDeadSpan = liveSpan.complete(revision);
        // dead span published first; liveSpan reference updated after
        deadSpans.add(nextDeadSpan);
        liveSpan = LiveSpan.empty(deadSpans.size());

        return true;
    }

    /**
     * Returns the revision data visible at {@code commitSeq}.
     * Not thread-safe; for writer use only.
     *
     * @param commitSeq  the target commit sequence
     * @return the revision data, or {@link Optional#empty()} if the key was
     *         deleted at or before {@code commitSeq}
     */
    public Optional<RevisionData> getAt(long commitSeq) {
        if (!liveSpan.isEmpty()) {
            var floor = Query.floorRevision(liveSpan.revisions(), commitSeq);
            if (floor >= 0) {
                return Optional.of(new RevisionData(
                        liveSpan.get(floor),
                        liveSpan.createdAt(),
                        liveSpan.versionAt(floor)
                ));
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

        // floor on the last revision means the key was deleted at or before commitSeq
        if (floor == span.revisions().size() - 1) {
            return Optional.empty();
        }

        return Optional.of(new RevisionData(span.get(floor), span.createdAt(), span.versionAt(floor)));
    }

    /**
     * Pinned-view implementation of {@link #getPinnedAt(long)}.
     *
     * <p>Bounds the dead span search to {@code Math.min(deadSpans.size()-1, liveSpan.position())}.
     * If the live span has since moved to dead, its pre-assigned position acts as the
     * inclusive upper bound - the reader sees exactly the spans that existed when its
     * view was established</p>
     */
    private Optional<RevisionData> getAt(
            VolatileList<DeadSpan>.PinnedView deadSpans,
            VolatileList<Revision>.PinnedView liveRevisions,
            LiveSpan pinnedLiveSpan,
            long commitSeq
    ) {
        if (!liveRevisions.isEmpty()) {
            var floor = Query.floorRevision(liveRevisions, commitSeq);
            if (floor >= 0) {
                return Optional.of(new RevisionData(
                        liveRevisions.get(floor),
                        pinnedLiveSpan.createdAt(),
                        pinnedLiveSpan.versionAt(floor)
                ));
            }
        }

        if (deadSpans.isEmpty()) {
            return Optional.empty();
        }

        // position is the upper bound: if liveSpan has moved to dead, its dead span is at
        // position and is included; spans beyond position are not part of this reader's view
        var spanFloor = Query.floorDeadSpan(deadSpans, Math.min(deadSpans.size() - 1, pinnedLiveSpan.position()), commitSeq);

        if (spanFloor < 0) {
            return Optional.empty();
        }

        var span = deadSpans.get(spanFloor);
        var floor = Query.floorRevision(span.revisions(), commitSeq);

        // floor on the last revision means the key was deleted at or before commitSeq
        if (floor == span.revisions().size() - 1) {
            return Optional.empty();
        }

        return Optional.of(new RevisionData(span.get(floor), span.createdAt(), span.versionAt(floor)));
    }

    /**
     * Returns the revision data visible at {@code commitSeq} from a consistent
     * point-in-time snapshot. Safe for concurrent readers.
     *
     * <p>The snapshot is established via three ordered volatile reads:
     * <ol>
     *   <li>Read the {@code liveSpan} reference - establishes which live span this reader observes.</li>
     *   <li>Pin the live span's revisions (volatile read of {@code size}) - establishes happens-before
     *       for all revisions and {@code createdAt} written before that volatile write.</li>
     *   <li>Pin {@code deadSpans} (volatile read of {@code size}) - captures the dead span array at this moment.</li>
     * </ol>
     * The writer advances the commit sequence with a volatile write only after all index mutations
     * are applied, so reading {@code liveSpan} guarantees all mutations up to that span are visible.</p>
     *
     * <p>The live span's pre-assigned position (P) bounds the dead span search. If the writer calls
     * {@link #tryComplete(Revision)} between steps 2 and 3, the live span moves to dead at index P
     * and a new live span appears at P+1. The reader's pinned dead spans will reflect this, but
     * the search is capped at P (inclusive) - the reader sees the completed span at P but nothing
     * written after its view was established.</p>
     *
     * @param commitSeq  the target commit sequence
     * @return the revision data, or {@link Optional#empty()} if the key was
     *         deleted at or before {@code commitSeq}
     */
    public Optional<RevisionData> getPinnedAt(long commitSeq) {
        // pin the liveSpan reference first - position() and pin() must come from the same instance
        var currLiveSpan = liveSpan;

        // volatile read of live revisions size; establishes happens-before for createdAt and all revisions
        var pinnedLiveSpan = currLiveSpan.pin();

        // interleaving point: writer may have called tryComplete() here, moving currLiveSpan to dead[P]
        // and creating a new live span at pos=P+1; pinnedDeadSpans will contain this advanced state -
        // position-bounded search in getAt() ensures the reader's view stays consistent
        //
        //   normal (no interleaving):
        //     deadSpans: [ dead[0] ] ... [ dead[P-1] ]   liveSpan: [ live(pos=P) ]
        //     reader searches all dead spans + live revisions
        //
        //   after interleaving:
        //     deadSpans: [ dead[0] ] ... [ dead[P-1] ] [ dead[P] ]   liveSpan: [ live(pos=P+1) ]
        //                                                 ^reader stops here
        //     live revisions (from currLiveSpan) are now empty; dead span search capped at pos=P
        var pinnedDeadSpans = deadSpans.pin();

        if (pinnedLiveSpan.isEmpty() && pinnedDeadSpans.isEmpty()) {
            throw new IllegalStateException("Inconsistent state: Timeline exists without a single span");
        }

        return getAt(pinnedDeadSpans, pinnedLiveSpan, currLiveSpan, commitSeq);
    }

    /** @return the first {@link Revision} in this timeline, across all spans */
    public Revision firstRevision() {
        return deadSpans.isEmpty() ? liveSpan.firstRevision() : deadSpans.getFirst().firstRevision();
    }

    /** @return the most recent {@link KeySpan} in this timeline */
    public KeySpan lastSpan() {
        return liveSpan.isEmpty() ? deadSpans.getLast() : liveSpan;
    }

    /**
     * Returns this timeline compacted to spans visible at or after {@code commitSeq}.
     * Dead spans before the compaction point are trimmed; the live span's position
     * is updated to reflect the new dead span array layout.
     *
     * @param commitSeq  the compaction boundary
     * @return the compacted timeline, or {@link Optional#empty()} if no spans survive
     */
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
                // if no live span and no dead span survives, drop the timeline
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
                // after compaction the prefix of dead spans may be removed;
                // live span position must be updated to its index in the new dead span array
                liveSpan.compact(commitSeq, uncompactedDeadSpans.size())
        ));
    }
}
