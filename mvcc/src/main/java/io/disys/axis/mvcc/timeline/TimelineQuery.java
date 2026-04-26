package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.model.Record;
import io.dsal.versioned.index.api.Direction;
import io.dsal.versioned.index.api.Range;
import io.dsal.versioned.index.api.ReadView;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Stateless query utilities over a {@link ReadView}{@code <byte[], KeyTimeline>}.
 * Provides range iteration, metadata filtering, value sorting, and pagination.
 */
public class TimelineQuery {

    /** @return the revision data for {@code key}, or empty if absent */
    public Optional<RevisionData> get(ReadView<byte[], KeyTimeline> view, byte[] key, Function<KeyTimeline, Optional<RevisionData>> mapper) {
        return view.get(key).flatMap(mapper);
    }

    /**
     * Streams results over {@code [from, to)} in {@code direction}, applying {@code mapper}
     * to each entry and discarding empty results.
     */
    public <T> Stream<T> range(ReadView<byte[], KeyTimeline> view, byte[] from, byte[] to, SortDirection direction, BiFunction<byte[], KeyTimeline, Optional<T>> mapper) {
        var stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        view.iterator(
                                direction == SortDirection.ASCENDING ? Direction.ASC : Direction.DESC,
                                Range.closedOpen(from, to),
                                mapper
                        ), Spliterator.ORDERED),
                false
        );

        return stream
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    /** Convenience overload returning a stream of {@link KeyRevisionData}. */
    public Stream<KeyRevisionData> range(ReadView<byte[], KeyTimeline> view, byte[] from, byte[] to, SortDirection direction, Function<KeyTimeline, Optional<RevisionData>> mapper) {
        return range(view, from, to, direction, (key, tl) -> mapper.apply(tl).map(rd -> new KeyRevisionData(key, rd)));
    }

    /**
     * Applies metadata filters (modifiedAt, createdAt, version) and non-value sorts
     * to {@code stream} before any record value is fetched.
     */
    public Stream<KeyRevisionData> preFilter(Stream<KeyRevisionData> stream, RangeOptions options) {
        if (options.hasModifiedFilter()) {
            stream = stream.filter(krd -> options.modifiedIn().test(krd.modifiedAt()));
        }
        if (options.hasCreatedFilter()) {
            stream = stream.filter(krd -> options.createdIn().test(krd.createdAt()));
        }
        if (options.hasVersionFilter()) {
            stream = stream.filter(krd -> options.versionIn().test(krd.version()));
        }
        if (!options.isKeySort() && options.sortTarget() != SortTarget.VAL) {
            stream = stream.sorted(keyRevisionComparator(options));
        }
        return stream;
    }

    /** Applies value-based sorting to {@code stream} after record values are fetched. */
    public Stream<Record> postFilter(Stream<Record> stream, RangeOptions options) {
        if (options.sortTarget() == SortTarget.VAL) {
            stream = stream.sorted(recordComparator(options));
        }
        return stream;
    }

    /** @return {@code true} if {@code options} requires a post-filter step */
    public boolean hasPostFilter(RangeOptions options) {
        return options.sortTarget() == SortTarget.VAL;
    }

    /**
     * Paginates {@code stream} to {@code limit} items.
     * The {@code more} flag in the result indicates whether further items exist.
     */
    public <T> Page<T> page(Stream<T> stream, long limit) {
        if (limit == RangeOptions.UNLIMITED) {
            return new Page<>(stream.toList(), false);
        }
        var items = stream.limit(limit + 1).toList();
        boolean more = items.size() > limit;
        return new Page<>(more ? items.subList(0, (int) limit) : items, more);
    }

    /** Full pipeline: pre-filter, fetch record values, post-filter, paginate. */
    public Page<Record> page(Stream<KeyRevisionData> stream, Function<KeyRevisionData, Record> fetch, RangeOptions options) {
        var records = postFilter(preFilter(stream, options).map(fetch), options);
        return page(records, options.limit());
    }

    /** Fetches and collects all items without filtering or pagination. */
    public Page<Record> page(Stream<KeyRevisionData> stream, Function<KeyRevisionData, Record> fetch) {
        return new Page<>(stream.map(fetch).toList(), false);
    }

    /** @return a page of all keys in {@code stream} without filtering or pagination */
    public Page<byte[]> pageKeys(Stream<KeyRevisionData> stream) {
        return new Page<>(stream.map(KeyRevisionData::key).toList(), false);
    }

    /** Full pipeline for keys: pre-filter, optional post-filter, paginate. */
    public Page<byte[]> pageKeys(Stream<KeyRevisionData> stream, Function<KeyRevisionData, Record> fetch, RangeOptions options) {
        stream = preFilter(stream, options);
        if (!hasPostFilter(options)) {
            return page(stream.map(KeyRevisionData::key), options.limit());
        }
        return page(postFilter(stream.map(fetch), options).map(Record::key), options.limit());
    }

    /** @return the count of entries in {@code stream} matching the filters in {@code options} */
    public long count(Stream<KeyRevisionData> stream, CountOptions options) {
        if (options.hasModifiedFilter()) {
            stream = stream.filter(krd -> options.modifiedIn().test(krd.modifiedAt()));
        }
        if (options.hasCreatedFilter()) {
            stream = stream.filter(krd -> options.createdIn().test(krd.createdAt()));
        }
        if (options.hasVersionFilter()) {
            stream = stream.filter(krd -> options.versionIn().test(krd.version()));
        }
        return stream.count();
    }

    /**
     * Comparator for pre-filter sorting. KEY order comes from the index iterator direction,
     * sorting here would be redundant. VAL requires the record value which is not available
     * before fetch; handled in {@link #postFilter}.
     */
    private Comparator<KeyRevisionData> keyRevisionComparator(RangeOptions options) {
        Comparator<KeyRevisionData> base = switch (options.sortTarget()) {
            case VERSION -> Comparator.comparingInt(KeyRevisionData::version);
            case CREATED_REVISION -> Comparator.comparingLong(KeyRevisionData::createdAt);
            case MODIFIED_REVISION -> Comparator.comparingLong(KeyRevisionData::modifiedAt);
            case KEY -> throw new IllegalStateException("KEY sort is handled by the index");
            case VAL -> throw new IllegalStateException("VAL sort is not applicable in pre-filter");
        };
        return options.sortDirection() == SortDirection.DESCENDING ? base.reversed() : base;
    }

    /** Comparator for post-filter sorting by value; only VAL sort reaches this stage. */
    private Comparator<Record> recordComparator(RangeOptions options) {
        Comparator<Record> base = Comparator.comparing(Record::val, Arrays::compare);
        return options.sortDirection() == SortDirection.DESCENDING ? base.reversed() : base;
    }
}
