package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

/**
 * The active {@link KeySpan} for a key that has not yet been deleted.
 */
public class LiveSpan implements KeySpan {
    /** Revision history for this span; backed by {@link VolatileList} for concurrent reads alongside ongoing writes. */
    private final VolatileList<Revision> revisions;

    /** Pre-assigned index in the dead span array where this span will be placed when completed; used by concurrent readers to locate the span. */
    private final int position;

    /** The commitSeq at which this span was created; {@code -1} for pre-allocated empty spans, set on the first {@link #add(Revision)}. */
    private long createdAt;

    /** The version of the first revision in this span; {@code final} for safe concurrent reads. */
    private final int startVersion;

    LiveSpan(VolatileList<Revision> revisions, int position, long createdAt, int startVersion) {
        this.revisions = revisions;
        this.position = position;
        this.createdAt = createdAt;
        this.startVersion = startVersion;
    }

    /** Creates a new span seeded with {@code revision} as its first entry. */
    static LiveSpan init(Revision revision) {
        return new LiveSpan(VolatileList.of(revision), 0, revision.commitSeq(), 1);
    }

    /** Restores a span from persisted state. */
    static LiveSpan restore(Revision revision, long createdAt, int startVersion) {
        return new LiveSpan(VolatileList.of(revision), 0, createdAt, startVersion);
    }

    /**
     * Returns a pre-allocated empty span before any revision is added.
     * {@code startVersion} is pre-set to {@code 1} - semantically valid only after the first {@link #add(Revision)}.
     *
     * @param position  the dead span array index
     */
    static LiveSpan empty(int position) {
        return new LiveSpan(VolatileList.allocate(10), position, -1, 1);
    }

    /** @return the dead span array index */
    int position() { return position; }

    /** @return the underlying revision list */
    VolatileList<Revision> revisions() { return revisions; }

    /** @return a pinned view of revisions for reader access */
    VolatileList<Revision>.PinnedView pin() {
        return revisions.pin();
    }

    /**
     * Appends {@code revision} to this span. Sets {@code createdAt} from the
     * revision's commitSeq if this span was empty.
     */
    void add(Revision revision) {
        if (isEmpty()) {
            // createdAt written only once during first revision addition
            // plain write before the volatile write to size in revisions.add() below;
            // createdAt should only be read after confirming revisions size
            // any reader calling pin() (volatile read of size) is guaranteed to see this value
            createdAt = revision.commitSeq();
        }
        revisions.add(revision);
    }

    @Override
    public long createdAt() {
        return createdAt;
    }

    @Override
    public long modifiedAt() {
        return revisions.getLast().commitSeq();
    }

    @Override
    public Revision firstRevision() {
        return revisions.getFirst();
    }

    @Override
    public Revision lastRevision() {
        return revisions.getLast();
    }

    @Override
    public int version() {
        // each revision adds one version step from startVersion
        return startVersion + revisions.size() - 1;
    }

    /** @return true if no revisions have been added yet */
    boolean isEmpty() {
        return revisions.isEmpty();
    }

    /** @return the version of the revision at index {@code idx} */
    public int versionAt(int idx) {
        // idx is the offset from startVersion within this span
        return startVersion + idx;
    }

    /** @return the {@link Revision} at index {@code idx} */
    Revision get(int idx) {
        return revisions.get(idx);
    }

    /** @return the number of revisions currently in this span */
    int size() { return revisions.size(); }

    /** Closes this span by appending {@code deleteRevision}, returning the resulting {@link DeadSpan}. */
    DeadSpan complete(Revision deleteRevision) {
        var nextDeadSpan = revisions.toList();
        nextDeadSpan.add(deleteRevision);
        return DeadSpan.create(nextDeadSpan, createdAt, startVersion);
    }

    /**
     * Retains the revision visible at {@code commitSeq} and everything after it,
     * dropping earlier history. Unlike a dead span, a live span always retains at
     * least the floor revision — the key is still alive and must remain answerable
     * at {@code commitSeq}.
     *
     * @param commitSeq  the compaction boundary
     * @param position   the position of this span in the new dead span array
     * @return the compacted span
     */
    LiveSpan compact(long commitSeq, int position) {
        var floor = Query.floorRevision(revisions, commitSeq);

        // span [r3, r6, r9], key still live:
        //   compact(2)  -> floor < 0, all revisions after compaction point -> unchanged
        //   compact(7)  -> floor = r6, drop r3, keep [r6, r9]
        //   compact(9)  -> floor = r9, keep [r9]
        if (floor < 0) {
            return new LiveSpan(revisions, position, createdAt, startVersion);
        }

        var compacted = revisions.copy(floor);

        return new LiveSpan(compacted, position, createdAt, startVersion + floor);
    }
}
