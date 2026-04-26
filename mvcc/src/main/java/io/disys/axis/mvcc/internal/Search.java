package io.disys.axis.mvcc.internal;

import java.util.function.BiFunction;
import java.util.function.Function;

/** Binary search utilities over indexed sequences. */
public class Search {

    /**
     * Finds the smallest index in {@code [left, right]} for which
     * {@code cmp.apply(index, t) >= 0}.
     *
     * @param cmp    index-based comparator against {@code t}; per {@link java.util.Comparator} convention
     * @param left   inclusive lower bound of the search range
     * @param right  inclusive upper bound of the search range
     * @param t      the target value
     * @return the smallest index satisfying the condition, or {@code right + 1} if none
     */
    public static <T> int lowerBound(BiFunction<Integer, T, Integer> cmp, int left, int right, T t) {

        while (left <= right) {
            var mid = left + (right - left) / 2;

            if (cmp.apply(mid, t) >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return left;
    }

    /**
     * Finds the largest index in {@code [left, right]} for which
     * {@code cmp.apply(index, t) <= 0}.
     *
     * @param cmp    index-based comparator against {@code t}; per {@link java.util.Comparator} convention
     * @param left   inclusive lower bound of the search range
     * @param right  inclusive upper bound of the search range
     * @param t      the target value
     * @return the largest index satisfying the condition, or {@code left - 1} if none
     */
    public static <T> int floor(BiFunction<Integer, T, Integer> cmp, int left, int right, T t) {

        while (left <= right) {
            var mid = left + (right - left) / 2;

            if (cmp.apply(mid, t) <= 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return right;
    }

    /**
     * Finds the index of the element equal to {@code t} in {@code [left, right]}.
     *
     * @param get    retrieves the element at a given index
     * @param left   inclusive lower bound of the search range
     * @param right  inclusive upper bound of the search range
     * @param t      the target value
     * @return the index of the matching element, or {@code -1} if not found
     */
    public static <T extends Comparable<T>> int find(Function<Integer, T> get, int left, int right, T t) {

        while (left <= right) {
            var mid = left + (right - left) / 2;
            var cmp = get.apply(mid).compareTo(t);

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
