package io.disys.axis.backend;

public class BackendException extends RuntimeException {
    public BackendException(String message) {
        super(message);
    }
    public BackendException(String message, Throwable cause) {
        super(message, cause);
    }
    public BackendException(Throwable cause) {
        super(cause);
    }
}
