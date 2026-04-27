package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.internal.Search;
import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

import java.util.Optional;

/**
 * Accumulates {@link RevisionRecord}s across logical commits before they are flushed
 * to the backend store. Backend commits are deferred and batched to avoid the cost of
 * frequent file flushes; records staged here become visible to readers immediately,
 * bridging the gap between logical commit and physical persistence.
 *
 * <p>Supports a single writer appending records in strictly increasing revision order.
 * Readers access a commit-seq-bounded {@link View} of the accumulated records.</p>
 */
public final class RevisionRecordBuffer {

    /** Backing list; stage/publish/pin model provides writer-reader isolation. */
    private final VolatileList<RevisionRecord> records;

    private RevisionRecordBuffer(VolatileList<RevisionRecord> records) {
        this.records = records;
    }

    /**
     * @param capacity the maximum number of records the buffer can hold
     * @return a new empty buffer with the given capacity
     * @throws IllegalArgumentException if {@code capacity} is not positive
     */
    public static RevisionRecordBuffer allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        return new RevisionRecordBuffer(VolatileList.allocate(capacity));
    }

    /**
     * @param record  the record to stage; its revision must be strictly after the last staged revision
     * @throws IllegalArgumentException if the revision order is violated
     */
    void stage(RevisionRecord record) {
        checkMonotonic(record);
        records.stage(record);
    }

    void checkMonotonic(RevisionRecord record) {
        var last = records.getLogicalLast();
        if (last != null && record.compareTo(last.revision()) <= 0) {
            throw new IllegalArgumentException(
                    "RevisionRecord's revision is not after the buffer's last revision");
        }
    }

    /** Makes all staged records atomically visible to readers. */
    void publish() {
        records.publish();
    }

    /** @return the number of published records */
    int size() {
        return records.size();
    }

    /** A pinned, commit-seq-bounded snapshot of the buffer; stable under concurrent writes. */
    public static final class View {
        /** Pinned snapshot; stable under concurrent writes. */
        private final VolatileList<RevisionRecord>.PinnedView pin;
        /** Inclusive index bounds of the visible range within {@code pin}. */
        private final int from;
        private final int to;

        // from and to both are inclusive
        private View(VolatileList<RevisionRecord>.PinnedView pin, int from, int to) {
            this.pin = pin;
            this.from = from;
            this.to = to;
        }

        boolean isEmpty() {
            return pin == null || from < 0 || to < from;
        }

        /**
         * @param target the revision to look up
         * @return the record at {@code target}, or empty if not present in this view
         */
        public Optional<RevisionRecord> get(Revision target) {
            if (isEmpty()) {
                return Optional.empty();
            }
            int pos = Search.find(idx -> pin.get(idx).revision(), from, to, target);
            return pos >= 0 ? Optional.of(pin.get(pos)) : Optional.empty();
        }
    }

    /**
     * @param target the revision to look up
     * @return the record at {@code target}, or empty if not present
     */
    public Optional<RevisionRecord> get(Revision target) {
        var left = 0;
        var right = records.size() - 1;

        int pos = Search.find(idx -> records.get(idx).revision(), left, right, target);
        return pos >= 0 ? Optional.of(records.get(pos)) : Optional.empty();
    }



    /** @return an empty view containing no records */
    public View emptyView() {
        return new View(null, -1, -1);
    }

    /**
     * @return a pinned view of records with commitSeq at or before {@code endSeq};
     *         empty if the buffer has no published records or all are after {@code endSeq}
     */
    public View view(long endSeq) {
        var pin = records.pin();
        if (pin.isEmpty()) {
            return emptyView();
        }

        if (pin.getFirst().compareTo(endSeq) > 0) {
            return emptyView();
        }

        // a commitSeq can have multiple ordinals; searching lowerBound(endSeq + 1) gives
        // the first record of the next seq, so end - 1 is the last ordinal of endSeq
        int end = lowerBound(pin, endSeq + 1);

        return new View(pin, 0, end - 1);
    }

    private int lowerBound(
            VolatileList<RevisionRecord>.PinnedView pin,
            long commitSeq
    ) {
        return Search.lowerBound(
                (idx, otherCommitSeq) -> pin.get(idx).compareTo(otherCommitSeq),
                0,
                pin.size() - 1,
                commitSeq
       );
    }

}
