package consensus.node;

import consensus.storage.Payload;

public interface TrackablePayload<ID> extends Trackable<ID>, Payload {
}
