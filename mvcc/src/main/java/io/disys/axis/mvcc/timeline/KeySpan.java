package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;

public interface KeySpan {
    long createdAtSeq();
    long modifiedAtSeq();

    Revision lastRevision();
    Revision firstRevision();

    int version();
}
