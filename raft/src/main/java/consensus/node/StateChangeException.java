package consensus.node;

import consensus.engine.VolatileState;

public class StateChangeException extends RuntimeException {
    public StateChangeException(VolatileState state) {
        super("Current node role is changed to " + state.role() + "; " + (state.leaderId().isEmpty() ? "leader is unknown" : "leader is changed to " + state.leaderId()));
    }
}
