package io.disys.axis.mvcc.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * A dynamically-growing array with a staged publication model for visibility across threads.
 *
 * <p>Elements accumulated via {@link #stage(T)} are not visible to readers until
 * {@link #publish()} makes them visible with a single volatile write to {@code size}.
 * Readers capture a consistent snapshot via {@link #pin()}, which performs a volatile
 * read of {@code size}.</p>
 *
 * <h2>Threading</h2>
 * <p>Concurrent writers are not supported. Multiple readers holding independent
 * {@link PinnedView} instances are safe.</p>
 */
public class VolatileList<T> {
    private static final int MIN_CAPACITY = 8;

    /**
     * The count of published elements. Volatile: the write in {@link #publish()} and
     * {@link #add(T)} establishes happens-before with any subsequent volatile read of
     * this field, ensuring published elements are visible to readers.
     */
    private volatile int size;

    /** Count of elements accumulated via {@link #stage(T)} but not yet published. */
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

    /**
     * An immutable snapshot of a {@link VolatileList} at the {@code size} captured
     * when {@link VolatileList#pin()} was called.
     */
    public class PinnedView {
        private final T[] list;
        private final int size;

        PinnedView(T[] list, int size) {
            this.list = list;
            this.size = size;
        }

        /** @return {@code true} if the snapshot contains no elements */
        public boolean isEmpty() {
            return size == 0;
        }

        /** @return the number of elements in this snapshot */
        public int size() {
            return size;
        }

        /**
         * @param idx  index within {@code [0, size)}
         * @return the element at {@code idx}
         * @throws IllegalArgumentException if {@code idx} is out of range
         */
        public T get(int idx) {
            if (idx < 0 || idx >= size) {
                throw new IllegalArgumentException("idx is not within range");
            }
            return list[idx];
        }

        /** @return the first element in this snapshot */
        public T getFirst() {
            return get(0);
        }

        /** @return the last element in this snapshot */
        public T getLast() {
            return get(size - 1);
        }
    }

    /**
     * Creates an empty list with at least {@code capacity} pre-allocated slots.
     *
     * @param capacity  the desired initial capacity
     * @return a new empty {@code VolatileList<T>}
     */
    public static <T> VolatileList<T> allocate(int capacity) {
        T[] list = (T[]) new Object[Math.max(capacity, MIN_CAPACITY)];
        return new VolatileList<>(list);
    }

    /**
     * Creates a list containing the given elements, all immediately published.
     *
     * @param e  the elements
     * @return a new {@code VolatileList<T>} with all elements published
     */
    public static <T> VolatileList<T> of(T ...e) {
        if (e.length >= MIN_CAPACITY) {
            return new VolatileList<>(e, e.length);
        }

        T[] list = (T[]) new Object[MIN_CAPACITY];
        System.arraycopy(e, 0, list, 0, e.length);
        return new VolatileList<>(list, e.length);
    }

    /**
     * Wraps an existing array without copying.
     *
     * @param list  the backing array
     * @param size  the number of valid elements in {@code list}
     * @return a new {@code VolatileList<T>} backed by {@code list}
     */
    public static <T> VolatileList<T> from(T[] list, int size) {
        return new VolatileList<>(list, size);
    }

    /**
     * Captures the current published state as an immutable {@link PinnedView}.
     * The volatile read of {@code size} ensures all elements published before
     * this call are visible in the returned view.
     *
     * @return a snapshot of the currently published elements
     */
    public PinnedView pin() {
        return new PinnedView(list, size);
    }

    /**
     * @return a {@link List} containing the currently published elements
     */
    public List<T> toList() {
        var newList = new ArrayList<T>(size);
        for (var i = 0; i < size; i++) {
            newList.add(list[i]);
        }
        return newList;
    }

    /** @return {@code true} if there are no published elements */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Accumulates {@code e} without publishing it. Not visible to readers
     * until {@link #publish()} is called.
     *
     * @param e  the element to stage
     */
    public void stage(T e) {
        resize();
        list[size + staged] = e;
        staged++;
    }

    /**
     * Publishes all staged elements with a single volatile write to {@code size},
     * making them visible to any reader that subsequently calls {@link #pin()}.
     */
    public void publish() {
        size += staged;
        staged = 0;
    }

    /** @return the count of published elements; volatile read */
    public int size() {
        return size;
    }

    /**
     * Returns a new {@code VolatileList<T>} containing the published elements
     * from index {@code from} inclusive to {@code size} exclusive.
     *
     * @param from  start index, inclusive; must be within {@code [0, size)}
     * @return a new list containing elements {@code [from, size)}
     * @throws IllegalArgumentException if {@code from} is out of range
     */
    public VolatileList<T> copy(int from) {
        if (from < 0 || from >= size) {
            throw new IllegalArgumentException();
        }
        var capacity = size - from + MIN_CAPACITY;
        var copy = (T[]) new Object[capacity];

        System.arraycopy(list, from, copy, 0, size - from);

        return VolatileList.from(copy, size - from);
    }

    /**
     * Publishes {@code e} directly as a single volatile write to {@code size}.
     * All staged elements must be committed via {@link #publish()} before calling this.
     *
     * @param e  the element to add
     */
    public void add(T e) {
        resize();
        list[size] = e;
        size++;
    }

    /**
     * @param idx  index within {@code [0, size)}
     * @return the element at {@code idx}
     * @throws IllegalArgumentException if {@code idx} is out of range
     */
    public T get(int idx) {
        if (idx < 0 || idx >= size) {
            throw new IllegalArgumentException("idx is not within range");
        }
        return list[idx];
    }

    /** @return the first published element */
    public T getFirst() {
        return get(0);
    }

    /** @return the last published element */
    public T getLast() {
        return get(size - 1);
    }

    /**
     * @return the last element across both published and staged elements,
     *         or {@code null} if both counts are zero
     */
    public T getLogicalLast() {
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
