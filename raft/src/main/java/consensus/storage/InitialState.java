package consensus.storage;

import consensus.membership.MembershipConfig;
import consensus.engine.PersistentState;

public record InitialState(PersistentState persistentState, MembershipConfig membershipConfig) {
}
