package io.disys.axis.mvcc.internal;

import java.util.function.BiFunction;
import java.util.function.Function;

public class Search {

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
