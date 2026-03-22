package io.disys.axis.wal;

public class WalClosedException extends Exception {
    public WalClosedException() {
        super("Cannot do operation on a closed Wal");
    }
}
