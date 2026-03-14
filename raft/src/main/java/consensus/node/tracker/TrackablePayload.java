package consensus.node.tracker;

import consensus.storage.Payload;

/**
 * A {@link Payload} that carries a unique identifier, allowing the
 * {@link DataProposalTracker} to match committed entries back to
 * their originating proposal futures.
 *
 * @param <ID> the identifier type
 */
public interface TrackablePayload<ID> extends Trackable<ID>, Payload {
}
