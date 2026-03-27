package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

// append only buffer for revision records
public class RevisionRecordBuffer {
    private final List<EncodedRevisionRecord> buffer;

    RevisionRecordBuffer() {
        buffer = new ArrayList<>();
    }

    boolean isEmpty() {
        return buffer.isEmpty();
    }

    int size() {
        return buffer.size();
    }

    void add(EncodedRevisionRecord record) {
        buffer.add(record);
    }

    int search(byte[] encodedRevision) {
        var left = 0;
        var right = buffer.size();

        while (left < right) {
            var mid = left + (right - left) / 2;
            var rev = buffer.get(mid);
            var cmp = rev.compareTo(encodedRevision);

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

    Optional<EncodedRevisionRecord> get(byte[] encodedRevision) {
        var idx = search(encodedRevision);
        return idx >= 0 ? Optional.of(buffer.get(idx)) : Optional.empty();
    }


}
