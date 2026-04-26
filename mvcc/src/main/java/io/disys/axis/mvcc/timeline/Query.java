package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.internal.Search;
import io.disys.axis.mvcc.model.Revision;

import java.util.List;


/** Binary search utilities over {@link DeadSpan} and {@link Revision} sequences within a timeline. */
public class Query {

    // ====== Dead span =====

    /** @return the largest index in {@code [0, right]} whose first revision is at or before {@code commitSeq}, or {@code -1} if none */
    static int floorDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int right, long commitSeq) {
        return floorDeadSpan(deadSpans, 0, right, commitSeq);
    }

    /** @return the largest index in {@code [0, size-1]} whose first revision is at or before {@code commitSeq}, or {@code -1} if none */
    static int floorDeadSpan(VolatileList<DeadSpan> deadSpans, long commitSeq) {
        return floorDeadSpan(deadSpans, 0, deadSpans.size() - 1, commitSeq);
    }

    /** @return the largest index in {@code [left, right]} whose first revision is at or before {@code commitSeq}, or {@code left - 1} if none */
    static int floorDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> deadSpans.get(idx).firstRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }

    /** @return the largest index in {@code [left, right]} whose first revision is at or before {@code commitSeq}, or {@code left - 1} if none */
    static int floorDeadSpan(VolatileList<DeadSpan> deadSpans, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> deadSpans.get(idx).firstRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }

    // ====== Revision ======

    /** @return the largest index in {@code [left, right]} whose revision is at or before {@code commitSeq}, or {@code left - 1} if none */
    static int floorRevision(VolatileList<Revision>.PinnedView revisions, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }

    /** @return the largest index in {@code [left, right]} whose revision is at or before {@code commitSeq}, or {@code left - 1} if none */
    static int floorRevision(List<Revision> revisions, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }

    /** @return the largest index in {@code [0, size-1]} whose revision is at or before {@code commitSeq}, or {@code -1} if none */
    static int floorRevision(VolatileList<Revision>.PinnedView revisions, long commitSeq) {
        return floorRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }

    /** @return the largest index in {@code [0, size-1]} whose revision is at or before {@code commitSeq}, or {@code -1} if none */
    static int floorRevision(List<Revision> revisions, long commitSeq) {
        return floorRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }

    /** @return the largest index in {@code [0, size-1]} whose revision is at or before {@code commitSeq}, or {@code -1} if none */
    static int floorRevision(VolatileList<Revision> revisions, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), 0, revisions.size() - 1, commitSeq);
    }

}
