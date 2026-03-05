package consensus.engine;

import consensus.algorithm.RoleType;
import consensus.algorithm.NodeId;

import java.util.Optional;

public record VolatileState (RoleType role, Optional<NodeId> leaderId) {
}
