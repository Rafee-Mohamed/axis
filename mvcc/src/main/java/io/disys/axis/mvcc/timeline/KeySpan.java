package io.disys.axis.mvcc.timeline;

import io.disys.axis.mvcc.io.*;
import io.disys.axis.mvcc.model.*;

public interface KeySpan {
    long createdAt();
    long modifiedAt();

    Revision lastRevision();
    Revision firstRevision();

    int version();
}
