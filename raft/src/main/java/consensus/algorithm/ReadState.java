package consensus.algorithm;

/**
 * A confirmed linearizable read index, surfaced to the application via Ready.
 *
 * <p>Once the leader confirms its authority (via heartbeat majority ack or
 * lease), the committed index at the time the read was registered becomes a
 * {@code ReadState}. The application can then serve any read whose data is
 * at or before this index — it is guaranteed that no other leader could have
 * committed a conflicting entry.</p>
 *
 * <p>The application must wait until its applied index {@code >=} this
 * {@code index} before responding to the client's read, to ensure the
 * state machine reflects all committed writes up to this point.</p>
 *
 * @param index the committed index at which the read is safe to serve
 */
public record ReadState(long index) {
}
