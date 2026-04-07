package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.internal.Search;
import io.disys.axis.mvcc.model.Revision;

import java.util.List;


public class Query {

    // ====== Dead span =====

    static int lowerBoundDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int right, long commitSeq) {
        return lowerBoundDeadSpan(deadSpans, 0, right, commitSeq);
    }

    static int floorDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int right, long commitSeq) {
        return floorDeadSpan(deadSpans, 0, right, commitSeq);
    }

    static int lowerBoundDeadSpan(VolatileList<DeadSpan> deadSpans, long commitSeq) {
        return lowerBoundDeadSpan(deadSpans, 0, deadSpans.size() - 1, commitSeq);
    }

    static int floorDeadSpan(VolatileList<DeadSpan> deadSpans, long commitSeq) {
        return floorDeadSpan(deadSpans, 0, deadSpans.size() - 1, commitSeq);
    }


    static int lowerBoundDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int left,  int right, long commitSeq) {
        return Search.lowerBound((idx, otherCommitSeq) -> deadSpans.get(idx).lastRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int floorDeadSpan(VolatileList<DeadSpan>.PinnedView deadSpans, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> deadSpans.get(idx).firstRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int lowerBoundDeadSpan(VolatileList<DeadSpan> deadSpans, int left, int right, long commitSeq) {
        return Search.lowerBound((idx, otherCommitSeq) -> deadSpans.get(idx).lastRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int floorDeadSpan(VolatileList<DeadSpan> deadSpans, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> deadSpans.get(idx).firstRevision().compareTo(otherCommitSeq), left, right, commitSeq);
    }



    // ====== Revision ======

    static int lowerBoundRevision(VolatileList<Revision>.PinnedView revisions, int left, int right,  long commitSeq) {
        return Search.lowerBound((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int floorRevision(VolatileList<Revision>.PinnedView revisions, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int lowerBoundRevision(List<Revision> revisions, int left, int right,  long commitSeq) {
        return Search.lowerBound((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }

    static int floorRevision(List<Revision> revisions, int left, int right, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), left, right, commitSeq);
    }


    static int lowerBoundRevision(VolatileList<Revision>.PinnedView revisions, long commitSeq) {
        return lowerBoundRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }

    static int floorRevision(VolatileList<Revision>.PinnedView revisions, long commitSeq) {
        return floorRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }


    static int lowerBoundRevision(List<Revision> revisions, long commitSeq) {
        return lowerBoundRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }

    static int floorRevision(List<Revision> revisions, long commitSeq) {
        return floorRevision(revisions, 0, revisions.size() - 1, commitSeq);
    }

    static int lowerBoundRevision(VolatileList<Revision> revisions, long commitSeq) {
        return Search.lowerBound((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), 0, revisions.size() - 1, commitSeq);
    }

    static int floorRevision(VolatileList<Revision> revisions, long commitSeq) {
        return Search.floor((idx, otherCommitSeq) -> revisions.get(idx).compareTo(otherCommitSeq), 0, revisions.size() - 1, commitSeq);
    }
    
}
