package consensus.algorithm;

/**
 * Determines how the leader confirms its authority before serving a
 * linearizable read.
 *
 * <p>Raft guarantees linearizable reads by ensuring the leader's commit index
 * is authoritative at the moment a read is served. The mode controls the
 * trade-off between latency, throughput, and safety assumptions.</p>
 *
 * <h4>Heartbeat modes vs Lease</h4>
 * <ul>
 *   <li><strong>Heartbeat modes</strong> confirm leadership by getting a
 *       majority ack on the network — safe under asynchronous assumptions
 *       (no clock dependency).</li>
 *   <li><strong>Lease mode</strong> assumes bounded clock drift and skips
 *       the network round-trip, serving reads immediately if the leader
 *       believes its lease is still valid.</li>
 * </ul>
 */
public enum ReadIndexMode {

    /**
     * Heartbeat-based confirmation: broadcast heartbeat immediately when a
     * read request arrives.
     *
     * <ul>
     *   <li><strong>Latency:</strong> one round-trip per read batch</li>
     *   <li><strong>Throughput:</strong> good — multiple reads between heartbeat
     *       send and ack are batched into the same seq and confirmed together</li>
     *   <li><strong>Safety:</strong> no clock assumptions</li>
     * </ul>
     */
    HEARTBEAT_IMMEDIATE,

    /**
     * Heartbeat-based confirmation: piggyback on the next periodic heartbeat
     * instead of sending one immediately.
     *
     * <ul>
     *   <li><strong>Latency:</strong> up to one heartbeat interval + round-trip</li>
     *   <li><strong>Throughput:</strong> best — no extra heartbeat traffic, all
     *       reads between two periodic heartbeats are confirmed together</li>
     *   <li><strong>Safety:</strong> no clock assumptions</li>
     * </ul>
     *
     * <p>Use when read latency tolerance is higher than a heartbeat interval
     * and minimizing network overhead is a priority.</p>
     */
    HEARTBEAT_TIMEOUT,

    /**
     * Lease-based reads: the leader serves reads immediately if it has
     * received heartbeat acks from a majority within the last election
     * timeout window, assuming its lease has not expired.
     *
     * <ul>
     *   <li><strong>Latency:</strong> zero — no network round-trip</li>
     *   <li><strong>Throughput:</strong> best — no extra messages at all</li>
     *   <li><strong>Safety:</strong> requires bounded clock drift across nodes.
     *       If clocks diverge beyond the assumed bound, stale reads are
     *       possible.</li>
     * </ul>
     *
     * <p>Requires {@code checkQuorum} to be enabled. The quorum check
     * provides the implicit lease window: the leader steps down if it
     * loses majority contact, and followers reject vote requests from
     * nodes with higher terms if a leader was recently heard from. This
     * combination guarantees no other leader can be elected while the
     * lease is believed valid.</p>
     */
    LEASE
}
