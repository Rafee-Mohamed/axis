package io.disys.axis.storage.swmr;

public class ValueStorage<V> {
    private final V[] vals;

    private ValueStorage(V[] vals) {
        this.vals = vals;
    }

    static <V> ValueStorage<V> of(V val) {
        return new ValueStorage<>((V[]) new Object[]{val});
    }

    int size() {
        return vals.length;
    }

    V val(int idx) {
        checkBounds(idx);
        return vals[idx];
    }


    ValueStorage<V> insert(int idx, V val) {
        checkInsertBounds(idx);
        var newVals = (V[]) new Object[vals.length + 1];

        System.arraycopy(vals, 0, newVals, 0, idx);
        newVals[idx] = val;
        System.arraycopy(vals, idx, newVals, idx + 1, vals.length - idx);

        return new ValueStorage<>(newVals);
    }

    ValueSplit<V> insertAndSplit(int insertIdx, int splitIdx, V val) {
        checkInsertBounds(insertIdx);
        checkSplitBounds(splitIdx);

        if (insertIdx >= splitIdx) {
            var leftVals = (V[]) new Object[splitIdx];
            System.arraycopy(vals, 0, leftVals, 0, splitIdx);

            var rightVals = (V[]) new Object[vals.length - splitIdx + 1];
            var prefixLen = insertIdx - splitIdx;
            var suffixLen = vals.length - insertIdx;

            System.arraycopy(vals, splitIdx, rightVals, 0, prefixLen);
            rightVals[prefixLen] = val;
            System.arraycopy(vals, insertIdx, rightVals, prefixLen + 1, suffixLen);

            return new ValueSplit<>(
                    new ValueStorage<>(leftVals),
                    new ValueStorage<>(rightVals)
            );
        }

        var leftVals = (V[]) new Object[splitIdx + 1];
        System.arraycopy(vals, 0, leftVals, 0, insertIdx);
        leftVals[insertIdx] = val;
        System.arraycopy(vals, insertIdx, leftVals, insertIdx + 1, splitIdx - insertIdx);

        var splitIdxAfterInsertion = splitIdx - 1;
        var rightVals = (V[]) new Object[vals.length - splitIdxAfterInsertion];
        System.arraycopy(vals, splitIdxAfterInsertion, rightVals, 0, rightVals.length);

        return new ValueSplit<>(
                new ValueStorage<>(leftVals),
                new ValueStorage<>(rightVals)
        );
    }

    private void checkSplitBounds(int idx) {
        if (idx <= 0 || idx > size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for split: " + "(" + 0 + " " + size() + ")");
        }
    }

    private void checkInsertBounds(int idx) {
        if (idx < 0 || idx > size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds for insert: " + "[" + 0 + " " + size() + ")");
        }
    }

    private void checkBounds(int idx) {
        if (idx < 0 || idx >= size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + size() + ")");
        }
    }
}
