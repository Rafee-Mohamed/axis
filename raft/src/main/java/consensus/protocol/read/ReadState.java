package consensus.protocol.read;

/**
 * A confirmed linearizable read, surfaced to the application via output.
 *
 * <p>A {@code ReadState} is only surfaced once two conditions are met
 * internally: (1) the leader confirmed its authority via heartbeat
 * quorum ack, and (2) the local state machine has applied up to at
 * least the committed index recorded at read registration time. The
 * application can serve the read immediately upon receiving this -
 * no further waiting is required.</p>
 *
 * @param index the committed index at which the read was registered
 */
public record ReadState(long index) {
}
