package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Revision;
import io.disys.axis.mvcc.model.SortDirection;
import io.dsal.versioned.index.api.Direction;
import io.dsal.versioned.index.api.Range;
import io.dsal.versioned.index.api.ReadView;

import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TimelineQuery {

    public Optional<RevisionData> get(ReadView<byte[], KeyTimeline> view, byte[] key, Function<KeyTimeline, Optional<RevisionData>> mapper) {
        return view.get(key).flatMap(mapper);
    }

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

    public Stream<KeyRevisionData> range(ReadView<byte[], KeyTimeline> view, byte[] from, byte[] to, SortDirection direction, Function<KeyTimeline, Optional<RevisionData>> mapper) {
        return range(view, from, to, direction, (key, tl) -> mapper.apply(tl).map(rd -> new KeyRevisionData(key, rd)));
    }
}
