package consensus.node;

import consensus.engine.VolatileState;

/**
 * Thrown to fail pending futures when the node's role changes.
 *
 * <p>Pending (uncommitted) proposals and unconfirmed reads may never
 * complete after a role transition, so their futures are failed with
 * this exception. The application can retry if appropriate.</p>
 */
public class StateChangeException extends RuntimeException {

    /**
     * @param state the new volatile state after the role change
     */
    public StateChangeException(VolatileState state) {
        super("Current node role is changed to " + state.role() + "; " + (state.leaderId().isEmpty() ? "leader is unknown" : "leader is changed to " + state.leaderId()));
    }
}
