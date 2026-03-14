package consensus.node.tracker;

/**
 * An object that exposes a unique identifier for tracking purposes.
 *
 * <p>The identifier type must implement {@code hashCode} and
 * {@code equals} consistently - the {@link DataProposalTracker}
 * uses it as a map key to match committed entries back to their
 * originating futures.</p>
 *
 * @param <ID> the identifier type, must have correct {@code hashCode}
 *             and {@code equals} semantics
 */
public interface Trackable<ID> {

    /**
     * Returns the unique identifier for this object.
     *
     * @return the identifier, never {@code null}
     */
    ID id();
}
