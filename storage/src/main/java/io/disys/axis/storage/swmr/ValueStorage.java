package io.disys.axis.storage.swmr;

import java.util.Arrays;

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
        checkIndexBounds(idx);
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

    ValueStorage<V> replace(int idx, V val) {
        checkIndexBounds(idx);

        var newVals = Arrays.copyOf(vals, vals.length);
        newVals[idx] = val;

        return new ValueStorage<>(newVals);
    }

    ValueStorage<V> remove(int idx) {
        checkIndexBounds(idx);
        var newVals = (V[]) new Object[vals.length - 1];

        System.arraycopy(vals, 0, newVals, 0, idx);
        System.arraycopy(vals, idx + 1, newVals, idx, vals.length - idx - 1);

        return new ValueStorage<>(newVals);
    }

    ValueStorage<V> removeAndInsert(int removeIdx, int insertIdx, V val) {
        checkIndexBounds(removeIdx);
        checkIndexBounds(insertIdx);

        var newVals = (V[]) new Object[vals.length];

        // prefix is unchanged before insertIdx or removeIdx
        var unchangedPrefixEnd = Math.min(removeIdx, insertIdx);
        System.arraycopy(vals, 0, newVals, 0, unchangedPrefixEnd);

        if (insertIdx > removeIdx) {
            System.arraycopy(vals, removeIdx + 1, newVals, removeIdx, insertIdx - removeIdx - 1);
            newVals[insertIdx] = val;
            System.arraycopy(vals, insertIdx, newVals, insertIdx + 1, vals.length - insertIdx);
        } else { // insert then remove, insertIdx == prefixEnd
            newVals[insertIdx] = val;
            System.arraycopy(vals, insertIdx, newVals, insertIdx + 1, removeIdx - insertIdx);
            System.arraycopy(vals, removeIdx + 1, newVals, removeIdx, vals.length - removeIdx - 1);
        }

        return new ValueStorage<>(newVals);
    }

    ValueStorage<V> merge(ValueStorage<V> other) {
        var newVals = (V[]) new Object[vals.length + other.size()];
        System.arraycopy(vals, 0, newVals, 0, vals.length);
        System.arraycopy(other.vals, 0, newVals, vals.length, other.size());

        return new ValueStorage<>(newVals);
    }

    ValueStorage<V> insertAndMerge(int insertIdx, V val, ValueStorage<V> other) {
        checkInsertBounds(insertIdx);

        var otherVals = other.vals;
        var newVals = (V[]) new Object[vals.length + otherVals.length + 1];

        System.arraycopy(vals, 0, newVals, 0, insertIdx);
        newVals[insertIdx] = val;
        System.arraycopy(vals, insertIdx, newVals, insertIdx + 1, vals.length - insertIdx);

        System.arraycopy(otherVals, 0, newVals, vals.length + 1, otherVals.length);

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

    private void checkIndexBounds(int idx) {
        if (idx < 0 || idx >= size()) {
            throw new IndexOutOfBoundsException("Index " + idx + " is out of bounds: " + "[" + 0 + " " + size() + ")");
        }
    }
}
