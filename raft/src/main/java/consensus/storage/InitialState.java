package consensus.storage;

import consensus.membership.MembershipConfig;
import consensus.node.PersistentState;

public record InitialState(PersistentState persistentState, MembershipConfig membershipConfig) {
}
