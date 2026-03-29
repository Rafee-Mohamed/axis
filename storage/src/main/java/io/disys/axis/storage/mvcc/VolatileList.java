package io.disys.axis.storage.mvcc;

import java.util.ArrayList;
import java.util.List;

public class VolatileList<T> {
    private static final int MIN_CAPACITY = 8;
    private volatile int size;
    private int staged;
    private T[] list;

    VolatileList(T[] list) {
        this.list = list;
        this.size = 0;
    }

    VolatileList(T[] list, int size) {
        this.list = list;
        this.size = size;
    }

    class PinnedView {
        private final T[] list;
        private final int size;


        PinnedView(T[] list, int size) {
            this.list = list;
            this.size = size;
        }

        int size() {
            return size;
        }

        T get(int idx) {
            if (idx < 0 || idx >= size) {
                throw new IllegalArgumentException("idx is not within range");
            }
            return list[idx];
        }


        T getFirst() {
            return get(0);
        }

        T getLast() {
            return get(size - 1);
        }
    }

    static <T> VolatileList<T> allocate(int capacity) {
        T[] list = (T[]) new Object[Math.max(capacity, MIN_CAPACITY)];
        return new VolatileList<>(list);
    }

    static <T> VolatileList<T> of(T ...e) {
        if (e.length >= MIN_CAPACITY) {
            return new VolatileList<>(e, e.length);
        }

        T[] list = (T[]) new Object[MIN_CAPACITY];
        System.arraycopy(e, 0, list, 0, e.length);
        return new VolatileList<>(list, e.length);
    }

    static <T> VolatileList<T> from(T[] list, int size) {
        return new VolatileList<>(list, size);
    }

    void release() {
        size += 0;
    }

    int acquire() {
        return size;
    }

    PinnedView pin() {
        return new PinnedView(list, size);
    }

    List<T> toList() {
        var newList = new ArrayList<T>(size);
        for (var i = 0; i < size; i++) {
            newList.add(list[i]);
        }
        return newList;
    }

    boolean isEmpty() {
        return size == 0;
    }

    void stage(T e) {
        resize();
        list[size + staged] = e;
        staged++;
    }

    void publish() {
        size += staged;
        staged = 0;
    }

    int size() {
        return size;
    }

    VolatileList<T> copy(int from) {
        if (from < 0 || from >= size) {
            throw new IllegalArgumentException();
        }
        // copy with min extra space
        var capacity = size - from + MIN_CAPACITY;
        var copy = (T[]) new Object[capacity];

        System.arraycopy(list, from, copy, 0, size - from);

        return VolatileList.from(copy, size - from);
    }


    // any staged elements should be published before add
    void add(T e) {
        resize();
        list[size] = e;
        size++;
    }

    T get(int idx) {
        if (idx < 0 || idx >= size) {
            throw new IllegalArgumentException("idx is not within range");
        }
        return list[idx];
    }

    T getFirst() {
        return get(0);
    }

    T getLast() {
        return get(size - 1);
    }

    T getLogicalLast() {
        var n = size + staged;
        if (n == 0) {
            return null;
        }
        return list[n - 1];
    }


    private void resize() {
        var n = size + staged;
        if (n < list.length) {
            return;
        }

        var newList = (T[]) new Object[list.length * 2];
        System.arraycopy(list, 0, newList, 0, list.length);
        list = newList;
    }
}
