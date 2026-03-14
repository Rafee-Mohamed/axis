package consensus.engine;

import consensus.protocol.role.RoleType;
import consensus.core.NodeId;

import java.util.Optional;

/**
 * Volatile state that the application may want to observe but does not
 * need to persist. Safe to lose on crash - rebuilt from the protocol.
 *
 * @param role     the node's current protocol role
 * @param leaderId the known leader, or empty if unknown
 */
public record VolatileState(RoleType role, Optional<NodeId> leaderId) {

    /**
     * Returns {@code true} if either field differs from the given values.
     *
     * @param role     the role to compare against
     * @param leaderId the leader id to compare against
     * @return {@code true} if any field has changed
     */
    public boolean hasChanged(RoleType role, Optional<NodeId> leaderId) {
        return this.role != role || !this.leaderId.equals(leaderId);
    }
}
