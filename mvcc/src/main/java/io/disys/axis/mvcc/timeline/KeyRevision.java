package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.model.Revision;

public record KeyRevision(byte[] key, Revision revision) {
}
