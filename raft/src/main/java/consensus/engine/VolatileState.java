package consensus.engine;

import consensus.protocol.role.RoleType;
import consensus.core.NodeId;

import java.util.Optional;

public record VolatileState (RoleType role, Optional<NodeId> leaderId) {
}
