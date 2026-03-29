package io.disys.axis.storage.mvcc;

import java.util.Optional;

public final class RevisionRecordBuffer {

    private final VolatileList<RevisionRecord> records;

    private RevisionRecordBuffer(VolatileList<RevisionRecord> records) {
        this.records = records;
    }

    static RevisionRecordBuffer allocate(int capacity) {
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
            throw new IllegalStateException(
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
            int idx = search(pin, target, from, to);
            return idx >= 0 ? Optional.of(pin.get(idx)) : Optional.empty();
        }
    }

    public View emptyView() {
        return new View(null, -1, -1);
    }

    public View view(long startSeq, long endSeq) {
        if (startSeq > endSeq) {
            throw new IllegalArgumentException("startSeq can't be greater than end Seq");
        }

        var pin = records.pin();
        if (pin.size() == 0) {
            return emptyView();
        }

        if (pin.getLast().compareTo(new Revision(startSeq, 0)) < 0
                || pin.getFirst().compareTo(new Revision(endSeq, 0)) > 0) {
            return emptyView();
        }

        int start = lowerBound(pin, new Revision(startSeq, 0));
        int end = lowerBound(pin, new Revision(endSeq + 1, 0));

        return new View(pin, start, end - 1);
    }

    private static int lowerBound(
            VolatileList<RevisionRecord>.PinnedView pin,
            Revision target
    ) {

        var left = 0;
        var right = pin.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (pin.get(mid).compareTo(target) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private static int search(
            VolatileList<RevisionRecord>.PinnedView pin,
            Revision target,
            int left,
            int right
    ) {
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int cmp = pin.get(mid).compareTo(target);
            if (cmp == 0) {
                return mid;
            }
            if (cmp > 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return -1;
    }

}
