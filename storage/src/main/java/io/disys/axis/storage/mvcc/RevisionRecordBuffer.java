package io.disys.axis.storage.mvcc;

import java.util.Optional;

public class RevisionRecordBuffer {
    private volatile RevisionRecord[] buffer;
    private volatile int size;
    private int staged;

    private RevisionRecordBuffer(RevisionRecord[] buffer) {
        this.buffer = buffer;
        this.size = 0;
    }

    static RevisionRecordBuffer allocate(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        return new RevisionRecordBuffer(new RevisionRecord[capacity]);
    }

    void stage(RevisionRecord record) {
        checkForStage(record);
        resize();
        buffer[size + staged] = record;
        staged++;
    }

    void publish() {
        size += staged;
        staged = 0;
    }

    int size() {
        return size;
    }

    private void checkForStage(RevisionRecord record) {
        var n = size + staged;
        if (n > 0 && record.compareTo(buffer[n - 1].revision()) <= 0) {
            throw new IllegalStateException(
                    "RevisionRecord's revision is not after the buffer's last revision");
        }
    }

    void resize() {
        if ((size + staged) < buffer.length) {
            return;
        }

        var newBuffer = new RevisionRecord[buffer.length * 2];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        buffer = newBuffer;
    }

    class View {
        private final int from;
        private final int to;
        private final RevisionRecord[] buffer;

        // from and to inclusive
        View(RevisionRecord[] buffer, int from, int to) {
            this.from = from;
            this.to = to;
            this.buffer = buffer;
        }

        boolean isEmpty() {
            return from < 0 || to < from;
        }

        Optional<RevisionRecord> get(Revision target) {
            if (isEmpty()) {
                return Optional.empty();
            }
            return RevisionRecordBuffer.this.get(buffer, target, from, to);
        }
    }

    View emptyView() {
        return new View(null, -1, -1);
    }

    View view(long startSeq, long endSeq) {
        if (startSeq > endSeq) {
            throw new IllegalArgumentException("startSeq can't be greater than end Seq");
        }

        // first pinning size - size change reflects only after buffer filled to that size
        var pinnedSize = size;
        // buffer is guaranteed to filled up to pinned size
        var pinnedBuffer = buffer;

        if (pinnedSize == 0) {
            return emptyView();
        }

        // if startSeq after last revision or endSeq is before first revision
        // then empty buffer
        if (pinnedBuffer[pinnedSize - 1].revision().compareTo(startSeq) < 0
                || pinnedBuffer[0].revision().compareTo(endSeq) > 0) {
            return emptyView();
        }

        var start = lowerBound(pinnedBuffer, new Revision(startSeq, 0), 0, pinnedSize - 1);
        // need the endSeq up to its last ordinal so search for its next seq
        var end = lowerBound(pinnedBuffer, new Revision(endSeq + 1, 0), 0, pinnedSize - 1);

        return new View(pinnedBuffer, start, end - 1);
    }

    private int lowerBound(RevisionRecord[] buffer, Revision target, int left, int right) {

        while (left <= right) {
            var mid = left + (right - left) / 2;

            if (buffer[mid].compareTo(target) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    private int search(RevisionRecord[] buffer, Revision target, int left, int right) {
        while (left <= right) {
            var mid = left + (right - left) / 2;
            var cmp = buffer[mid].compareTo(target);

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

    private Optional<RevisionRecord> get(RevisionRecord[] buffer, Revision revision, int left, int right) {
        var idx = search(buffer, revision, left, right);
        return idx >= 0 ? Optional.of(buffer[idx]) : Optional.empty();
    }
}
