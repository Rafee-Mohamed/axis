package io.disys.axis.mvcc.io;

import io.disys.axis.mvcc.internal.Search;
import io.disys.axis.mvcc.internal.VolatileList;
import io.disys.axis.mvcc.model.*;

import java.util.Optional;

public final class RevisionRecordBuffer {

    private final VolatileList<RevisionRecord> records;

    private RevisionRecordBuffer(VolatileList<RevisionRecord> records) {
        this.records = records;
    }

    public static RevisionRecordBuffer allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        return new RevisionRecordBuffer(VolatileList.allocate(capacity));
    }

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

    void publish() {
        records.publish();
    }

    int size() {
        return records.size();
    }

    public static final class View {
        private final VolatileList<RevisionRecord>.PinnedView pin;
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

        public Optional<RevisionRecord> get(Revision target) {
            if (isEmpty()) {
                return Optional.empty();
            }
            int pos = Search.find(idx -> pin.get(idx).revision(), from, to, target);
            return pos >= 0 ? Optional.of(pin.get(pos)) : Optional.empty();
        }
    }

    public Optional<RevisionRecord> get(Revision target) {
        var left = 0;
        var right = records.size() - 1;

        int pos = Search.find(idx -> records.get(idx).revision(), left, right, target);
        return pos >= 0 ? Optional.of(records.get(pos)) : Optional.empty();
    }



    public View emptyView() {
        return new View(null, -1, -1);
    }

    public View view(long startSeq, long endSeq) {
        if (startSeq > endSeq) {
            throw new IllegalArgumentException("startSeq can't be greater than end Seq");
        }

        var pin = records.pin();
        if (pin.isEmpty()) {
            return emptyView();
        }

        if (pin.getLast().compareTo(startSeq) < 0
                || pin.getFirst().compareTo(endSeq) > 0) {
            return emptyView();
        }

        int start = lowerBound(pin,startSeq);
        // to capture up to the last ordinal of endSeq,
        // therefore searching for next one. last ordinal
        // ends at end - 1
        int end = lowerBound(pin, endSeq + 1);

        return new View(pin, start, end - 1);
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
