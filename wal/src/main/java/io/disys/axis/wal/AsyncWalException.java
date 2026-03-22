package io.disys.axis.wal;

public sealed class AsyncWalException extends Exception permits
        AsyncWalException.GracefulShutdown,
        AsyncWalException.Terminated,
        AsyncWalException.UnexpectedState {

    AsyncWalException(String message) { super(message); }
    AsyncWalException(String message, Throwable cause) { super(message, cause); }

    public static final class GracefulShutdown extends AsyncWalException {
        public GracefulShutdown() { super("WAL closed gracefully"); }
    }

    public static final class Terminated extends AsyncWalException {
        public Terminated(Throwable cause) { super("WAL terminated due to fatal error", cause); }
    }

    public static final class UnexpectedState extends AsyncWalException {
        public UnexpectedState(State expected, State actual) {
            super("Expected WAL to be in " + expected + " but was " + actual);
        }
    }
}
