package io.disys.axis.mvcc.error;

import io.disys.axis.mvcc.model.Revision;


import java.util.HexFormat;

public sealed class InconsistentStoreException extends RuntimeException
        permits InconsistentStoreException.MissingRecordForRevision {

    public InconsistentStoreException(String message) {
        super(message);
    }

    public static final class MissingRecordForRevision extends InconsistentStoreException {

        private final byte[] key;
        private final Revision revision;
        private final long firstCommitSeq;
        private final long lastCommitSeq;

        public MissingRecordForRevision(byte[] key, Revision revision, long firstCommitSeq, long lastCommitSeq) {
            super("Record missing for timeline-selected revision: key(hex)=%s, revision=%s, readerBounds=[%d..%d] inclusive commitSeq"
                    .formatted(HexFormat.of().formatHex(key), revision, firstCommitSeq, lastCommitSeq));
            this.key = key;
            this.revision = revision;
            this.firstCommitSeq = firstCommitSeq;
            this.lastCommitSeq = lastCommitSeq;
        }

        public byte[] key() {
            return key;
        }

        public Revision revision() {
            return revision;
        }

        public long firstCommitSeq() {
            return firstCommitSeq;
        }

        public long lastCommitSeq() {
            return lastCommitSeq;
        }
    }
}
