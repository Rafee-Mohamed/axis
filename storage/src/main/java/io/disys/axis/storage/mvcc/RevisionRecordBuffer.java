package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

// append only buffer for revision records
public class RevisionRecordBuffer {
    private final List<RevisionRecord> buffer;

    RevisionRecordBuffer() {
        this(new ArrayList<>());
    }

    RevisionRecordBuffer(List<RevisionRecord> buffer) {
        this.buffer = buffer;
    }

    class View {
        private final int from;
        private final int to;

        // from and to inclusive
        View(int from, int to) {
            this.from = from;
            this.to = to;
        }

        boolean isEmpty() {
            return from < 0 || to < from;
        }


        Optional<RevisionRecord> get(Revision target) {
            if (isEmpty()) return Optional.empty();
            return RevisionRecordBuffer.this.get(target, from, to);
        }

    }

    View emptyView() {
        return new View(-1, -1);
    }

    View view(long startSeq, long endSeq) {
        if (startSeq > endSeq) {
            throw new IllegalArgumentException("startSeq can't be greater than end Seq");
        }

        // if startSeq after last revision or endSeq is before first revision
        // then empty buffer
        if (
                buffer.isEmpty() ||
                buffer.getLast().revision().compareTo(startSeq) < 0 ||
                buffer.getFirst().revision().compareTo(endSeq) > 0
        ) {
            return emptyView();
        }


        var start = lowerBound(new Revision(startSeq, 0));


        // need the endSeq up to its last ordinal so search for its next seq
        var end = lowerBound(new Revision(endSeq + 1, 0));


        return new View(start, end - 1);
    }

    int size() {
        return buffer.size();
    }

    void add(RevisionRecord record) {
        if (!buffer.isEmpty() && record.compareTo(buffer.getLast().revision()) <= 0) {
            throw new IllegalStateException(
                    "RevisionRecord's revision is not after the buffer's last revision"
            );
        }
        buffer.add(record);
    }

    // first revision record >= target
    int lowerBound(Revision target) {
        return lowerBound(target, 0, buffer.size() - 1);
    }

    // first revision record >= target
    int lowerBound(Revision target, int left, int right) {

        while (left <= right) {
            var mid = left + (right - left) / 2;

            if (buffer.get(mid).compareTo(target) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    int search(Revision target) {
        return search(target, 0, buffer.size() - 1);
    }


    int search(Revision target, int left, int right) {
        while (left <= right) {
            var mid = left + (right - left) / 2;
            var cmp = buffer.get(mid).compareTo(target);

            if (cmp == 0) {
                return mid;
            }

            if (cmp > 0) {
                right = mid -1;
            } else {
                left = mid + 1;
            }
        }

        return -1;
    }

    Optional<RevisionRecord> get(Revision revision) {
        return get(revision, 0, buffer.size() - 1);
    }

    Optional<RevisionRecord> get(Revision revision, int left, int right) {
        var idx = search(revision, left, right);
        return idx >= 0 ? Optional.of(buffer.get(idx)) : Optional.empty();
    }


}
