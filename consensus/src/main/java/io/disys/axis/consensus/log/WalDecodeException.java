package io.disys.axis.consensus.log;

/**
 * Thrown when a WAL record cannot be parsed as a valid proto message.
 */
public final class WalDecodeException extends RuntimeException {

    public WalDecodeException(Throwable cause) {
        super(cause);
    }
}
