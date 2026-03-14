package consensus.protocol.read;

import consensus.core.NodeId;
import consensus.cluster.membership.MembershipConfig;

import java.util.*;

/**
 * Tracks pending linearizable read requests waiting for leadership
 * confirmation via heartbeat sequence numbers.
 *
 * <h2>State</h2>
 * <pre>
 *   seq           - monotonic counter, incremented once per heartbeat broadcast
 *   pendingReads  - FIFO queue of reads waiting for quorum ack
 *   peerAckedSeq  - per-peer high-water mark of acked heartbeat seq
 * </pre>
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>Read arrives        - addPending(from, commitIndex) stores Pending{from, commitIndex, seq+1}</li>
 *   <li>Leader broadcasts   - nextSeq() increments seq, attaches it to Heartbeat message</li>
 *   <li>Followers echo seq  - onHeartbeatAck(peer, ackedSeq) records the ack</li>
 *   <li>Quorum acked?       - drainAcked() confirms and returns reads</li>
 * </ol>
 *
 * <h2>Pending queue example</h2>
 * <pre>
 *   seq = 2 at the time reads arrive, then two heartbeat broadcasts happen:
 *
 *   R1, R2, R3 arrive (seq=2)  -> stored with requiredSeq = 3  (seq+1)
 *   nextSeq()                  -> seq becomes 3, Heartbeat(seq=3) sent
 *   R4, R5 arrive (seq=3)     -> stored with requiredSeq = 4  (seq+1)
 *   nextSeq()                  -> seq becomes 4, Heartbeat(seq=4) sent
 *
 *   requiredSeq:  3       3       3       4       4
 *              +-------+-------+-------+-------+-------+
 *              |  R1   |  R2   |  R3   |  R4   |  R5   |
 *              +-------+-------+-------+-------+-------+
 *                front                             back
 *                 ^                        ^
 *            drain when               drain when
 *          quorumAcked >= 3         quorumAcked >= 4
 * </pre>
 *
 * <h2>Why seq + 1</h2>
 * <pre>
 *   Problem: a read arrives when seq = 5.
 *            Heartbeat(seq=5) was already sent before this read.
 *            Acks for seq=5 prove leadership at the time of that
 *            heartbeat, not after the read was registered.
 *
 *   Fix:     store requiredSeq = seq + 1 = 6.
 *            The read is only confirmed by Heartbeat(seq=6) or later,
 *            which is guaranteed to be sent after the read arrived.
 *
 *   Timeline:
 *     HB(5) sent ... read arrives (seq=5) ... HB(6) sent ... acks for 6
 *                     |                                       |
 *                requiredSeq = 6                        confirmed here
 * </pre>
 *
 * <h2>How drainAcked works</h2>
 * <pre>
 *   3-node cluster: leader=A, peers={B, C}
 *
 *   peerAckedSeq:  { B: 3, C: 0 }   (B acked seq=3, C hasn't responded)
 *   leader A:      Long.MAX_VALUE    (leader always counts itself)
 *
 *   Step 1 - compute quorumAckedSeq (same algorithm as quorumAgreed):
 *
 *     sorted acked values:  [0,  3,  MAX]
 *                                ^
 *                          quorum position = size(3) - quorumSize(2) = index 1
 *                          quorumAckedSeq = 3
 *
 *   Step 2 - drain from front while pending.seq <= quorumAckedSeq:
 *
 *     requiredSeq:  3       3       3       4       4
 *                +-------+-------+-------+-------+-------+
 *                |  R1   |  R2   |  R3   |  R4   |  R5   |
 *                +-------+-------+-------+-------+-------+
 *                 3 <= 3   3 <= 3   3 <= 3  4 > 3  stop
 *                |-------- drain --------||---- stay ----|
 *
 *     Result: R1, R2, R3 confirmed. R4, R5 stay in queue.
 *
 *   Later, C acks seq=4:
 *     peerAckedSeq: { B: 3, C: 4 }
 *     sorted: [3, 4, MAX] -> quorumAckedSeq = 4
 *
 *                +-------+-------+
 *                |  R4   |  R5   |
 *                +-------+-------+
 *                 4 <= 4   4 <= 4 -> both drained
 * </pre>
 *
 * <h2>Why {@code <=} for the drain check</h2>
 * <p>The check is {@code pending.seq <= quorumAckedSeq}. A pending read
 * with requiredSeq = N needs a quorum to have acked at least seq N. If
 * quorumAckedSeq = N, that means a quorum has seen Heartbeat(seq=N) or
 * later. That heartbeat was sent after the read was registered (because
 * of the seq+1 assignment), so leadership is confirmed. Equal is
 * sufficient.</p>
 *
 * <h2>Invariants</h2>
 * <ul>
 *   <li>seq is strictly monotonically increasing (incremented once per
 *       broadcast)</li>
 *   <li>Pending.seq values in the queue are monotonically non-decreasing
 *       (FIFO ordering + seq only grows)</li>
 *   <li>peerAckedSeq per peer never decreases ({@code Math.max} on
 *       every update)</li>
 *   <li>Once a Pending cannot be drained (its seq > quorumAcked), no
 *       later Pending can either, so draining stops at the first
 *       failure</li>
 *   <li>All pending reads belong to a single term. ReadIndex is created
 *       fresh when a node becomes leader. On term change the old
 *       ReadIndex and all its pending reads are discarded. There is no
 *       cross-term contamination.</li>
 * </ul>
 */
public class ReadIndex {

    /**
     * A pending read awaiting leadership confirmation.
     *
     * @param from  node that requested the read (leader's own id or a
     *              follower's id for forwarded reads)
     * @param index committed index when registered - becomes the read
     *              index returned to the application
     * @param seq   minimum heartbeat seq that a quorum must ack to
     *              confirm this read
     */
    public record Pending(NodeId from, long index, long seq) {};

    /** Monotonic heartbeat sequence counter. Incremented once per broadcast. */
    private long seq;

    /** FIFO queue of pending reads, ordered by non-decreasing seq. */
    private final Queue<Pending> pendingReads;

    /** Per-peer high-water mark: highest heartbeat seq each peer has acked. */
    private final Map<NodeId, Long> peerAckedSeq;

    /**
     * Creates an empty ReadIndex with seq=0, no pending reads, and no
     * acked peers.
     */
    public ReadIndex() {
        seq = 0;
        pendingReads = new ArrayDeque<>();
        peerAckedSeq = new HashMap<>();
    }

    /**
     * Increments and returns the heartbeat sequence number. Called when
     * broadcasting a {@link consensus.message.Message.Heartbeat} - the
     * returned value is attached to the message and echoed back by
     * followers.
     *
     * @return the new sequence number for the outgoing heartbeat
     */
    public long nextSeq() {
        return ++seq;
    }

    /**
     * Registers a pending read at the current commit index.
     *
     * <p>The read records {@code seq + 1} as its required confirmation
     * seq. This ensures the read is only confirmed by a heartbeat sent
     * <em>after</em> this registration - not by a stale response from
     * a heartbeat sent before the read arrived.</p>
     *
     * @param id        the requesting node (leader's own id for local
     *                  reads, follower id for forwarded reads)
     * @param readIndex the leader's commit index at registration time
     */
    public void addPending(NodeId id, long readIndex) {
        pendingReads.add(new Pending(id, readIndex, seq + 1));
    }

    /**
     * Records a peer's heartbeat acknowledgment. Uses {@code Math.max}
     * so the tracked seq never goes backwards - out-of-order responses
     * are handled correctly.
     *
     * @param id       the peer that responded
     * @param ackedSeq the heartbeat seq echoed in the response
     */
    public void onHeartbeatAck(NodeId id, long ackedSeq) {
        peerAckedSeq.merge(id, ackedSeq, Math::max);
    }

    /**
     * Drains all pending reads that have been confirmed by a quorum of
     * heartbeat acknowledgments.
     *
     * <p>Computes the quorum-agreed seq using the same sorted-quorum
     * algorithm as {@link MembershipConfig#quorumAgreed} - sort each
     * voter's acked seq, pick the value at the quorum position. This
     * gives the highest seq that at least a quorum has acknowledged.
     * The leader is always counted as having acked
     * {@code Long.MAX_VALUE} (it trivially confirms its own
     * leadership).</p>
     *
     * <p>Then drains the FIFO queue from the front: all pending reads
     * whose required seq {@code <=} the quorum-agreed seq are confirmed.
     * Since the queue is monotonically non-decreasing by seq, once a
     * read cannot be confirmed no later read can either.</p>
     *
     * @param leader the leader's node id
     * @param mc     the current membership config for quorum calculation
     * @return confirmed reads to respond to (empty list if none)
     */
    public List<Pending> drainAcked(NodeId leader, MembershipConfig mc) {
        if (pendingReads.isEmpty()) {
            return List.of();
        }

        var quorumAckedSeq = mc.quorumAgreed(voter ->
                voter.equals(leader) ? Long.MAX_VALUE : peerAckedSeq.getOrDefault(voter, 0L),
                Long.MAX_VALUE
        );

        var ackedReads = new ArrayList<Pending>();
        while (!pendingReads.isEmpty() && pendingReads.peek().seq() <= quorumAckedSeq) {
            ackedReads.add(pendingReads.poll());
        }
        return ackedReads;
    }
}
