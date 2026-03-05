package consensus.node;

import consensus.algorithm.Role;
import consensus.algorithm.RoleType;

public record VolatileState (NodeId id, RoleType role) {
}
