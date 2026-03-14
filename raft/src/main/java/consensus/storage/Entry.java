package consensus.storage;

import consensus.cluster.membership.MembershipChanges;

import java.util.List;

/**
 * A single entry in the Raft log.
 *
 * <p>Every entry carries a {@code term} (the leader term that created it)
 * and an {@code index} (its position in the log). The sealed variants
 * distinguish the kinds of entries the protocol produces:</p>
 *
 * <ul>
 *   <li>{@link Placeholder} - sentinel at the log's base (snapshot boundary)</li>
 *   <li>{@link Data} - client-proposed data carrying a {@link Payload}</li>
 *   <li>{@link MembershipChange} - cluster membership reconfiguration</li>
 *   <li>{@link LeaveJoint} - marker to exit joint consensus</li>
 * </ul>
 */
public sealed interface Entry {

    /**
     * Returns the leader term that created this entry.
     *
     * @return term number
     */
    long term();

    /**
     * Returns the position of this entry in the log.
     *
     * @return log index
     */
    long index();

    /**
     * Uniquely identifies a log position by its term and index.
     *
     * @param term  the term of the entry
     * @param index the log index
     */
    record Id(long term, long index) {}

    /**
     * Returns the serialized size of this entry in bytes.
     * Defaults to zero for entries without a data payload.
     *
     * @return size in bytes
     */
    default long size() { return 0; }

    /**
     * Returns the total serialized size of the given entries in bytes.
     *
     * @param entries entries to measure
     * @return aggregate size in bytes
     */
    static long calculateSize(List<Entry> entries) {
        return entries.stream().mapToLong(Entry::size).sum();
    }

    /**
     * Sentinel entry at the log's base. Occupies the index covered by the
     * most recent snapshot and carries no data - it exists so that
     * {@code term(firstIndex - 1)} is always available for log matching.
     *
     * @param term  the term at this index
     * @param index the log index
     */
    record Placeholder(long term, long index) implements Entry {}

    /**
     * Client-proposed data entry carrying an application-defined {@link Payload}.
     *
     * @param term  the leader term that created this entry
     * @param index the log index
     * @param data  the application payload
     */
    record Data(long term, long index, Payload data) implements Entry {
        @Override
        public long size() { return data.bytes(); }
    }

    /**
     * Membership reconfiguration entry that transitions the cluster
     * into a new configuration.
     *
     * @param term              the leader term that created this entry
     * @param index             the log index
     * @param membershipChanges the requested membership changes
     */
    record MembershipChange(long term, long index, MembershipChanges membershipChanges) implements Entry {}

    /**
     * Marker entry that exits joint consensus, transitioning the cluster
     * from a joint configuration to the new single configuration.
     *
     * @param term  the leader term that created this entry
     * @param index the log index
     */
    record LeaveJoint(long term, long index) implements Entry {}
}

