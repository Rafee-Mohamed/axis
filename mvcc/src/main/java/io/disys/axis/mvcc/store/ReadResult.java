package io.disys.axis.mvcc.store;

import io.disys.axis.mvcc.codec.*;
import io.disys.axis.mvcc.error.*;
import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;
import io.disys.axis.mvcc.timeline.*;

public sealed interface ReadResult {
    record Present(io.disys.axis.mvcc.model.Record record) implements ReadResult {}
    record Absent() implements ReadResult {}
    record Compacted(long firstVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
    record Future(long lastVisibleCommitSeq, long requestedCommitSeq) implements ReadResult {}
}
