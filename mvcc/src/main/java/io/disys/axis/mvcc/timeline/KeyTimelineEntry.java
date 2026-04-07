package io.disys.axis.mvcc.timeline;

public record KeyTimelineEntry(byte[] key, KeyTimeline timeline) {
}
